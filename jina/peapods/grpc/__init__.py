import argparse
import asyncio
from threading import Thread
from typing import Optional, Callable, List, Dict, Union

import grpc
from google.protobuf import struct_pb2
from kubernetes import watch
from kubernetes.client import CoreV1Api

from jina.logging.logger import JinaLogger
from jina.proto import jina_pb2_grpc, jina_pb2
from jina.types.message import Message
from jina.types.message.common import ControlMessage
from jina.types.routing.table import RoutingTable


class Grpclet(jina_pb2_grpc.JinaDataRequestRPCServicer):
    """A `Grpclet` object can send/receive Messages via gRPC.

    :param args: the parsed arguments from the CLI
    :param message_callback: the callback to call on received messages
    :param logger: the logger to use
    """

    def __init__(
        self,
        args: argparse.Namespace,
        message_callback: Callable[['Message'], None],
        logger: Optional['JinaLogger'] = None,
        connection_pool: 'GrpcK8sConnectionPool' = None,
    ):
        self.args = args
        self._logger = logger or JinaLogger(self.__class__.__name__)
        self.callback = message_callback
        if connection_pool:
            self._connection_pool = connection_pool
        else:
            self._connection_pool = GrpcK8sConnectionPool()
        self.msg_recv = 0
        self.msg_sent = 0
        self._pending_tasks = []
        self._send_routing_table = args.send_routing_table
        self._logger.info('### has routing table?')
        if not args.send_routing_table:
            self._logger.info('### args.routing_table', args.routing_table)
            self._routing_table = RoutingTable(args.routing_table)
            self._next_targets = self._routing_table.get_next_target_addresses()
        else:
            self._routing_table = None
            self._next_targets = None

    async def send_message(self, msg: 'Message', **kwargs):
        """
        Sends a message via gRPC to the target indicated in the message's routing table
        :param msg: the protobuf message to send
        :param kwargs: Additional arguments.
        """

        if self._next_targets:
            for pod_address in self._next_targets:
                self._send_message(msg, pod_address)
        else:
            routing_table = RoutingTable(msg.envelope.routing_table)
            next_targets = routing_table.get_next_targets()
            for target, _ in next_targets:
                self._send_message(
                    self._add_envelope(msg, target),
                    target.active_target_pod.full_address,
                )

    def _send_message(self, msg, pod_address):
        try:
            self.msg_sent += 1

            self._pending_tasks.append(
                self._connection_pool.send_message(msg, pod_address)
            )

            self._update_pending_tasks()
        except grpc.RpcError as ex:
            self._logger.error('Sending data request via grpc failed', ex)
            raise ex

    @staticmethod
    def send_ctrl_msg(pod_address: str, command: str, timeout=1.0):
        """
        Sends a control message via gRPC to pod_address
        :param pod_address: the pod to send the command to
        :param command: the command to send (TERMINATE/ACTIVATE/...)
        :param timeout: optional timeout for the request in seconds
        :returns: Empty protobuf struct
        """
        stub = Grpclet._create_grpc_stub(pod_address, is_async=False)
        response = stub.Call(ControlMessage(command), timeout=timeout)
        return response

    @staticmethod
    def _create_grpc_stub(pod_address, is_async=True):
        if is_async:
            channel = grpc.aio.insecure_channel(
                pod_address,
                options=[
                    ('grpc.max_send_message_length', -1),
                    ('grpc.max_receive_message_length', -1),
                ],
            )
        else:
            channel = grpc.insecure_channel(
                pod_address,
                options=[
                    ('grpc.max_send_message_length', -1),
                    ('grpc.max_receive_message_length', -1),
                ],
            )

        stub = jina_pb2_grpc.JinaDataRequestRPCStub(channel)

        return stub

    def _add_envelope(self, msg, routing_table):
        if self._send_routing_table:
            new_envelope = jina_pb2.EnvelopeProto()
            new_envelope.CopyFrom(msg.envelope)
            new_envelope.routing_table.CopyFrom(routing_table.proto)
            new_message = Message(request=msg.request, envelope=new_envelope)

            return new_message
        else:
            return msg

    async def close(self, grace_period=None, *args, **kwargs):
        """Stop the Grpc server
        :param grace_period: Time to wait for message processing to finish before killing the grpc server
        :param args: Extra positional arguments
        :param kwargs: Extra key-value arguments
        """
        self._update_pending_tasks()
        self._connection_pool.close()
        try:
            await asyncio.wait_for(asyncio.gather(*self._pending_tasks), timeout=1.0)
        except asyncio.TimeoutError:
            self._update_pending_tasks()
            self._logger.warning(
                f'Could not gracefully complete {len(self._pending_tasks)} pending tasks on close.'
            )
        self._logger.debug('Close grpc server')
        await self._grpc_server.stop(grace_period)

    async def start(self):
        """
        Starts this Grpclet by starting its gRPC server
        """
        self._grpc_server = grpc.aio.server(
            options=[
                ('grpc.max_send_message_length', -1),
                ('grpc.max_receive_message_length', -1),
            ]
        )

        jina_pb2_grpc.add_JinaDataRequestRPCServicer_to_server(self, self._grpc_server)
        bind_addr = f'{self.args.host}:{self.args.port_in}'
        self._grpc_server.add_insecure_port(bind_addr)
        self._logger.debug(f'Binding gRPC server for data requests to {bind_addr}')
        await self._grpc_server.start()
        await self._grpc_server.wait_for_termination()

    def _update_pending_tasks(self):
        self._pending_tasks = [task for task in self._pending_tasks if not task.done()]

    async def Call(self, msg, *args):
        """Processes messages received by the GRPC server
        :param msg: The received message
        :param args: Extra positional arguments
        :return: Empty protobuf struct, necessary to return for protobuf Empty
        """
        if self.callback:
            self._pending_tasks.append(asyncio.create_task(self.callback(msg)))
        else:
            self._logger.debug(
                'Grpclet received data request, but no callback was registered'
            )

        self.msg_recv += 1
        self._update_pending_tasks()
        return struct_pb2.Value()


class ConnectionPool:
    """
    Maintains a list of connections and uses round roubin for selecting a connection

    :param port: port to use for the connections
    """

    def __init__(self, port: int):
        self.port = port
        self._connections = []
        self._ip_to_stub_idx = {}
        self._rr_counter = 0

    def add_connection(self, ip: str, connection):
        """
        Add connection with ip to the connection pool
        :param ip: Target IP of this connection
        :param connection: The connection to add
        """
        if ip not in self._ip_to_stub_idx:
            self._ip_to_stub_idx[ip] = len(self._connections)
            self._connections.append(connection)

    def remove_connection(self, ip: str):
        """
        Remove connection with ip from the connection pool
        :param ip: Remove connection for this ip
        :returns: The removed connection or None if there was not any for the given ip
        """
        if ip in self._ip_to_stub_idx:
            return self._connections.pop(self._ip_to_stub_idx.pop(ip))

        return None

    def get_connection(self):
        """
        Returns a connection from the pool. Strategy is round robin
        :returns: A connection from the pool
        """
        stub = self._connections[self._rr_counter]
        self._rr_counter = (self._rr_counter + 1) % len(self._connections)
        return stub

    def has_connection(self, ip: str) -> bool:
        """
        Checks if a connection for ip exists in the pool
        :param ip: The ip to check
        :returns: True if a connection for the ip exists in the pool
        """
        return ip in self._ip_to_stub_idx


class K8sConnectionPool:
    """
    Manages connections to replicas in a K8s deployment.

    :param namespace: K8s namespace the deployments redide in
    :param deployments: deployments to manage connections for, needs to have (deployment) name and head_host, head_port_in
    :param client: K8s client
    """

    def __init__(
        self,
        namespace: str = None,
        deployments: List[Dict[str, Union[str, int]]] = None,
        client: CoreV1Api = None,
        enabled=False,
        logger: JinaLogger = None,
    ):
        if enabled and not client:
            raise ValueError('K8sConnectionPool enabled, but no client was provided')

        self._namespace = namespace
        self._deployment_hostaddresses = {}
        self._connections = {}
        self._ip_port = {}
        self._k8s_client = client
        self.enabled = enabled
        self._logger = logger or JinaLogger(self.__class__.__name__)

        if deployments:
            self.deployments = deployments
        else:
            self.deployments = {}
        for deployment in self.deployments:
            self._deployment_hostaddresses[deployment['name']] = deployment['head_host']
            self._connections[deployment['head_host']] = ConnectionPool(
                deployment['head_port_in']
            )

        self._fetch_initial_state()

        self.update_thread = Thread(target=self.run)
        self.update_thread.start()

    def _fetch_initial_state(self):
        if self.enabled:
            namespaced_pods = self._k8s_client.list_namespaced_pod(self._namespace)
            for item in namespaced_pods.items:
                self._process_item(item)
        else:
            for target in self._connections:
                connection_pool = self._connections[target]
                connection_pool.add_connection(
                    target,
                    self._create_connection(target=f'{target}:{connection_pool.port}'),
                )

    def run(self):
        """
        Subscribes on MODIFIED events from list_namespaced_pod AK8s PI
        """
        while self.enabled:
            w = watch.Watch()
            for event in w.stream(
                self._k8s_client.list_namespaced_pod, self._namespace
            ):
                if event['type'] == 'MODIFIED':
                    self._process_item(event['object'])

    def send_message(self, msg: Message, deployment_address: str):
        """Send msg to deployment_address via one of the pooled connections

        :param msg: message to send
        :param deployment_address: address to send to, should include the port like 1.1.1.1:53
        :return: result of the actual send method
        """
        host = deployment_address[: deployment_address.rfind(':')]
        if host in self._connections:
            pooled_connection = self._connections[host].get_connection()
            return self._send_message(msg, pooled_connection)
        elif not self.enabled:
            # If the pool is disabled and an unknown connection is requested: create it
            connection_pool = self._create_connection_pool(deployment_address, host)
            return self._send_message(msg, connection_pool.get_connection())
        else:
            raise ValueError(f'Unknown address {deployment_address}')

    def _create_connection_pool(self, deployment_address, host):
        port = deployment_address[deployment_address.rfind(':') + 1 :]
        connection_pool = ConnectionPool(port=port)
        connection_pool.add_connection(
            host, self._create_connection(target=deployment_address)
        )
        self._connections[host] = connection_pool
        return connection_pool

    def close(self):
        """
        Closes the connection pool
        """
        self._connections.clear()
        self.enabled = False

    def _send_message(self, msg: Message, connection):
        raise NotImplementedError

    def _create_connection(self, target):
        raise NotImplementedError

    @staticmethod
    def _pod_is_up(item):
        return item.status.pod_ip is not None and item.status.phase == 'Running'

    def _process_item(self, item):
        deployment_name = item.metadata.labels["app"]
        is_deleted = item.metadata.deletion_timestamp is not None

        if not is_deleted and K8sConnectionPool._pod_is_up(item):
            if deployment_name in self._deployment_hostaddresses:
                target = item.status.pod_ip
                if not self._connections[
                    self._deployment_hostaddresses[deployment_name]
                ].has_connection(target):
                    self._logger.debug(
                        f'Adding connection to {target} for deployment {deployment_name} at {self._deployment_hostaddresses[deployment_name]}'
                    )

                    connection_pool = self._connections[
                        self._deployment_hostaddresses[deployment_name]
                    ]
                    connection_pool.add_connection(
                        target,
                        self._create_connection(
                            target=f'{target}:{connection_pool.port}'
                        ),
                    )
            else:
                self._logger.debug(
                    f'Observed state change in unknown deployment {deployment_name}'
                )
        elif is_deleted and K8sConnectionPool._pod_is_up(item):
            target = item.status.pod_ip
            if self._connections[
                self._deployment_hostaddresses[deployment_name]
            ].has_connection(target):
                self._logger.debug(
                    f'Removing connection to {target} for deployment {deployment_name} at {self._deployment_hostaddresses[deployment_name]}'
                )
                self._connections[
                    self._deployment_hostaddresses[deployment_name]
                ].remove_connection(target)


class GrpcK8sConnectionPool(K8sConnectionPool):
    """
    K8sConnectionPool which uses gRPC as the communication mechanism
    """

    def _send_message(self, msg: Message, connection):
        # this wraps the awaitable object from grpc as a coroutine so it can be used as a task
        # the grpc call function is not a coroutine but some _AioCall
        async def task_wrapper(new_message, stub):
            await stub.Call(new_message)

        return asyncio.create_task(task_wrapper(msg, connection))

    def _create_connection(self, target):
        self._logger.debug(f'create connection to {target}')
        channel = grpc.aio.insecure_channel(
            target,
            options=[
                ('grpc.max_send_message_length', -1),
                ('grpc.max_receive_message_length', -1),
            ],
        )

        return jina_pb2_grpc.JinaDataRequestRPCStub(channel)
