use crate::accounts_data::{AccountDataCache, AccountDataError};
use crate::announce_accounts::AnnounceAccountCache;
use crate::client::{
    BlockApproval, ChunkEndorsementMessage, ClientSenderForNetwork, ProcessTxRequest,
    TxStatusRequest, TxStatusResponse,
};
use crate::concurrency::demux;
use crate::concurrency::runtime::Runtime;
use crate::config;
use crate::network_protocol::{
    Edge, EdgeState, PartialEdgeInfo, PeerIdOrHash, PeerInfo, PeerMessage, RawRoutedMessage,
    RoutedMessage, SignedAccountData, SnapshotHostInfo, T1MessageBody, T2MessageBody,
    TieredMessageBody,
};
use crate::peer::peer_actor::ClosingReason;
use crate::peer::peer_actor::PeerActor;
use crate::peer_manager::connection;
use crate::peer_manager::connection_store;
use crate::peer_manager::peer_store;
use crate::private_actix::RegisterPeerError;
#[cfg(feature = "distance_vector_routing")]
use crate::routing::NetworkTopologyChange;
use crate::routing::route_back_cache::RouteBackCache;
use crate::shards_manager::ShardsManagerRequestFromNetwork;
use crate::snapshot_hosts::{SnapshotHostInfoError, SnapshotHostsCache};
use crate::state_witness::{
    ChunkContractAccessesMessage, ChunkStateWitnessAckMessage, ContractCodeRequestMessage,
    ContractCodeResponseMessage, PartialEncodedContractDeploysMessage,
    PartialEncodedStateWitnessForwardMessage, PartialEncodedStateWitnessMessage,
    PartialWitnessSenderForNetwork,
};
use crate::stats::metrics;
use crate::store;
use crate::tcp;
use crate::types::{
    ChainInfo, PeerManagerSenderForNetwork, PeerType, ReasonForBan, StateHeaderRequestBody,
    StatePartRequestBody, StateRequestSenderForNetwork, Tier3Request, Tier3RequestBody,
};
use anyhow::Context;
use arc_swap::ArcSwap;
use near_async::messaging::{CanSend, SendAsync, Sender};
use near_async::time;
use near_o11y::span_wrapped_msg::SpanWrappedMessageExt;
use near_primitives::genesis::GenesisId;
use near_primitives::hash::CryptoHash;
use near_primitives::network::PeerId;
use near_primitives::types::AccountId;
use parking_lot::{Mutex, RwLock};
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use tracing::Instrument as _;

mod routing;
mod tier1;

/// Limit number of pending Peer actors to avoid OOM.
pub(crate) const LIMIT_PENDING_PEERS: usize = 60;

/// Size of LRU cache size of recent routed messages.
/// It should be large enough to detect duplicates (i.e. all messages received during
/// production of 1 block should fit).
const RECENT_ROUTED_MESSAGES_CACHE_SIZE: usize = 10000;

/// How long a peer has to be unreachable, until we prune it from the in-memory graph.
const PRUNE_UNREACHABLE_PEERS_AFTER: time::Duration = time::Duration::hours(1);

/// Remove the edges that were created more that this duration ago.
pub const PRUNE_EDGES_AFTER: time::Duration = time::Duration::minutes(30);

/// How long to wait between reconnection attempts to the same peer
pub(crate) const RECONNECT_ATTEMPT_INTERVAL: time::Duration = time::Duration::seconds(10);

impl WhitelistNode {
    pub fn from_peer_info(pi: &PeerInfo) -> anyhow::Result<Self> {
        Ok(Self {
            id: pi.id.clone(),
            addr: if let Some(addr) = pi.addr {
                addr
            } else {
                anyhow::bail!("address is missing");
            },
            account_id: pi.account_id.clone(),
        })
    }
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct WhitelistNode {
    id: PeerId,
    addr: SocketAddr,
    account_id: Option<AccountId>,
}

pub(crate) struct NetworkState {
    /// Dedicated runtime for `NetworkState` which runs in a separate thread.
    /// Async methods of NetworkState are not cancellable,
    /// so calling them from, for example, PeerActor is dangerous because
    /// PeerActor can be stopped at any moment.
    /// WARNING: DO NOT spawn infinite futures/background loops on this arbiter,
    /// as it will be automatically closed only when the NetworkState is dropped.
    /// WARNING: actix actors can be spawned only when actix::System::current() is set.
    /// DO NOT spawn actors from a task on this runtime.
    runtime: Runtime,
    /// PeerManager config.
    pub config: config::VerifiedConfig,
    /// When network state has been constructed.
    pub created_at: time::Instant,
    /// GenesisId of the chain.
    pub genesis_id: GenesisId,
    pub client: ClientSenderForNetwork,
    pub state_request_adapter: StateRequestSenderForNetwork,
    pub peer_manager_adapter: PeerManagerSenderForNetwork,
    pub shards_manager_adapter: Sender<ShardsManagerRequestFromNetwork>,
    pub partial_witness_adapter: PartialWitnessSenderForNetwork,

    /// Network-related info about the chain.
    pub chain_info: ArcSwap<Option<ChainInfo>>,
    /// AccountsData for TIER1 accounts.
    pub accounts_data: Arc<AccountDataCache>,
    /// AnnounceAccounts mapping TIER1 account ids to peer ids.
    pub account_announcements: Arc<AnnounceAccountCache>,
    /// Connected peers (inbound and outbound) with their full peer information.
    pub tier2: connection::Pool,
    pub tier1: connection::Pool,
    pub tier3: connection::Pool,
    /// Semaphore limiting inflight inbound handshakes.
    pub inbound_handshake_permits: Arc<tokio::sync::Semaphore>,
    /// The public IP of this node; available after connecting to any one peer.
    pub my_public_addr: Arc<RwLock<Option<std::net::SocketAddr>>>,
    /// Peer store that provides read/write access to peers.
    pub peer_store: peer_store::PeerStore,
    /// Information about state snapshots hosted by network peers.
    pub snapshot_hosts: Arc<SnapshotHostsCache>,
    /// Connection store that provides read/write access to stored connections.
    pub connection_store: connection_store::ConnectionStore,
    /// List of peers to which we should re-establish a connection
    pub pending_reconnect: Mutex<Vec<PeerInfo>>,
    /// A graph of the whole NEAR network.
    pub graph: Arc<crate::routing::Graph>,
    /// A sparse graph of the whole NEAR network.
    /// TODO(saketh): deprecate graph above, rename this to RoutingTable
    #[cfg(feature = "distance_vector_routing")]
    pub graph_v2: Arc<crate::routing::GraphV2>,

    /// Hashes of the body of recently received routed messages.
    /// It allows us to determine whether messages arrived faster over TIER1 or TIER2 network.
    pub recent_routed_messages: Mutex<lru::LruCache<CryptoHash, ()>>,

    /// Hash of messages that requires routing back to respective previous hop.
    pub tier2_route_back: Mutex<RouteBackCache>,
    /// Currently unused, as TIER1 messages do not require a response.
    /// Also TIER1 connections are direct by design (except for proxies),
    /// so routing shouldn't really be needed.
    /// TODO(gprusak): consider removing it altogether.
    pub tier1_route_back: Mutex<RouteBackCache>,

    /// Shared counter across all PeerActors, which counts number of `RoutedMessageBody::ForwardTx`
    /// messages since last block.
    pub txns_since_last_block: AtomicUsize,

    /// Whitelisted nodes, which are allowed to connect even if the connection limit has been
    /// reached.
    whitelist_nodes: Vec<WhitelistNode>,

    /// Mutex which prevents overlapping calls to tier1_advertise_proxies.
    tier1_advertise_proxies_mutex: tokio::sync::Mutex<()>,
    /// Demultiplexer aggregating calls to add_edges(), for V1 routing protocol
    add_edges_demux: demux::Demux<Vec<Edge>, Result<(), ReasonForBan>>,
    /// Demultiplexer aggregating calls to update_routes(), for V2 routing protocol
    #[cfg(feature = "distance_vector_routing")]
    update_routes_demux:
        demux::Demux<crate::routing::NetworkTopologyChange, Result<(), ReasonForBan>>,

    /// Mutex serializing calls to set_chain_info(), which mutates a bunch of stuff non-atomically.
    /// TODO(gprusak): make it use synchronization primitives in some more canonical way.
    set_chain_info_mutex: Mutex<()>,
}

impl NetworkState {
    pub fn new(
        clock: &time::Clock,
        store: store::Store,
        peer_store: peer_store::PeerStore,
        config: config::VerifiedConfig,
        genesis_id: GenesisId,
        client: ClientSenderForNetwork,
        state_request_adapter: StateRequestSenderForNetwork,
        peer_manager_adapter: PeerManagerSenderForNetwork,
        shards_manager_adapter: Sender<ShardsManagerRequestFromNetwork>,
        partial_witness_adapter: PartialWitnessSenderForNetwork,
        whitelist_nodes: Vec<WhitelistNode>,
    ) -> Self {
        Self {
            runtime: Runtime::new(),
            graph: Arc::new(crate::routing::Graph::new(crate::routing::GraphConfig {
                node_id: config.node_id(),
                prune_unreachable_peers_after: PRUNE_UNREACHABLE_PEERS_AFTER,
                prune_edges_after: Some(PRUNE_EDGES_AFTER),
            })),
            #[cfg(feature = "distance_vector_routing")]
            graph_v2: Arc::new(crate::routing::GraphV2::new(crate::routing::GraphConfigV2 {
                node_id: config.node_id(),
                prune_edges_after: Some(PRUNE_EDGES_AFTER),
            })),
            genesis_id,
            client,
            state_request_adapter,
            peer_manager_adapter,
            shards_manager_adapter,
            partial_witness_adapter,
            chain_info: Default::default(),
            tier2: connection::Pool::new(config.node_id()),
            tier1: connection::Pool::new(config.node_id()),
            tier3: connection::Pool::new(config.node_id()),
            inbound_handshake_permits: Arc::new(tokio::sync::Semaphore::new(LIMIT_PENDING_PEERS)),
            my_public_addr: Arc::new(RwLock::new(None)),
            peer_store,
            snapshot_hosts: Arc::new(SnapshotHostsCache::new(config.snapshot_hosts.clone())),
            connection_store: connection_store::ConnectionStore::new(store.clone()).unwrap(),
            pending_reconnect: Mutex::new(Vec::<PeerInfo>::new()),
            accounts_data: Arc::new(AccountDataCache::new()),
            account_announcements: Arc::new(AnnounceAccountCache::new(store)),
            tier2_route_back: Mutex::new(RouteBackCache::default()),
            tier1_route_back: Mutex::new(RouteBackCache::default()),
            recent_routed_messages: Mutex::new(lru::LruCache::new(
                NonZeroUsize::new(RECENT_ROUTED_MESSAGES_CACHE_SIZE).unwrap(),
            )),
            txns_since_last_block: AtomicUsize::new(0),
            whitelist_nodes,
            add_edges_demux: demux::Demux::new(config.routing_table_update_rate_limit),
            #[cfg(feature = "distance_vector_routing")]
            update_routes_demux: demux::Demux::new(config.routing_table_update_rate_limit),
            set_chain_info_mutex: Mutex::new(()),
            config,
            created_at: clock.now(),
            tier1_advertise_proxies_mutex: tokio::sync::Mutex::new(()),
        }
    }

    /// Spawn a future on the runtime which has the same lifetime as the NetworkState instance.
    /// In particular if the future contains the NetworkState handler, it will be run until
    /// completion. It is safe to self.spawn(...).await.unwrap(), since runtime will be kept alive
    /// by the reference to self.
    ///
    /// It should be used to make the public methods cancellable: you spawn the
    /// noncancellable logic on self.runtime and just await it: in case the call is cancelled,
    /// the noncancellable logic will be run in the background anyway.
    fn spawn<R: 'static + Send>(
        &self,
        fut: impl std::future::Future<Output = R> + 'static + Send,
    ) -> tokio::task::JoinHandle<R> {
        self.runtime.handle.spawn(fut.in_current_span())
    }

    /// Stops peer instance if it is still connected,
    /// and then mark peer as banned in the peer store.
    pub fn disconnect_and_ban(
        &self,
        clock: &time::Clock,
        peer_id: &PeerId,
        ban_reason: ReasonForBan,
    ) {
        let tier2 = self.tier2.load();
        if let Some(peer) = tier2.ready.get(peer_id) {
            peer.stop(Some(ban_reason));
        } else {
            if let Err(err) = self.peer_store.peer_ban(clock, peer_id, ban_reason) {
                tracing::debug!(target: "network", ?err, "Failed to save peer data");
            }
        }
    }

    /// is_peer_whitelisted checks whether a peer is a whitelisted node.
    /// whitelisted nodes are allowed to connect, even if the inbound connections limit has
    /// been reached. This predicate should be evaluated AFTER the Handshake.
    pub fn is_peer_whitelisted(&self, peer_info: &PeerInfo) -> bool {
        self.whitelist_nodes
            .iter()
            .filter(|wn| wn.id == peer_info.id)
            .filter(|wn| Some(wn.addr) == peer_info.addr)
            .any(|wn| wn.account_id.is_none() || wn.account_id == peer_info.account_id)
    }

    /// predicate checking whether we should allow an inbound connection from peer_info.
    fn is_inbound_allowed(&self, peer_info: &PeerInfo) -> bool {
        // Check if we have spare inbound connections capacity.
        let tier2 = self.tier2.load();
        if tier2.ready.len() + tier2.outbound_handshakes.len() < self.config.max_num_peers as usize
            && !self.config.inbound_disabled
        {
            return true;
        }
        // Whitelisted nodes are allowed to connect, even if the inbound connections limit has
        // been reached.
        if self.is_peer_whitelisted(peer_info) {
            return true;
        }
        false
    }

    /// Register a direct connection to a new peer. This will be called after successfully
    /// establishing a connection with another peer. It becomes part of the connected peers.
    ///
    /// To build new edge between this pair of nodes both signatures are required.
    /// Signature from this node is passed in `edge_info`
    /// Signature from the other node is passed in `full_peer_info.edge_info`.
    pub async fn register(
        self: &Arc<Self>,
        clock: &time::Clock,
        edge: Edge,
        conn: Arc<connection::Connection>,
    ) -> Result<(), RegisterPeerError> {
        let this = self.clone();
        let clock = clock.clone();
        self.spawn(async move {
            let peer_info = &conn.peer_info;
            // Check if this is a blacklisted peer.
            if peer_info.addr.as_ref().map_or(true, |addr| this.peer_store.is_blacklisted(addr)) {
                tracing::debug!(target: "network", peer_info = ?peer_info, "Dropping connection from blacklisted peer or unknown address");
                return Err(RegisterPeerError::Blacklisted);
            }

            if this.peer_store.is_banned(&peer_info.id) {
                tracing::debug!(target: "network", id = ?peer_info.id, "Dropping connection from banned peer");
                return Err(RegisterPeerError::Banned);
            }

            match conn.tier {
                tcp::Tier::T1 => {
                    if conn.peer_type == PeerType::Inbound {
                        if !this.config.tier1.enable_inbound {
                            return Err(RegisterPeerError::Tier1InboundDisabled);
                        }
                        // Allow for inbound TIER1 connections only directly from a TIER1 peers.
                        let owned_account = conn.owned_account.as_ref().ok_or(RegisterPeerError::NotTier1Peer)?;
                        if !this.accounts_data.load().keys.contains(&owned_account.account_key) {
                            return Err(RegisterPeerError::NotTier1Peer);
                        }
                    }
                    if !edge.verify() {
                        return Err(RegisterPeerError::InvalidEdge);
                    }
                    this.tier1.insert_ready(conn).map_err(RegisterPeerError::PoolError)?;
                }
                tcp::Tier::T2 => {
                    if conn.peer_type == PeerType::Inbound {
                        if !this.is_inbound_allowed(&peer_info) {
                            // TODO(1896): Gracefully drop inbound connection for other peer.
                            let tier2 = this.tier2.load();
                            tracing::debug!(target: "network",
                                tier2 = tier2.ready.len(), outgoing_peers = tier2.outbound_handshakes.len(),
                                max_num_peers = this.config.max_num_peers,
                                "Dropping handshake (network at max capacity)."
                            );
                            return Err(RegisterPeerError::ConnectionLimitExceeded);
                        }
                    }
                    // First verify and broadcast the edge of the connection, so that in case
                    // it is invalid, the connection is not added to the pool.
                    // TODO(gprusak): consider actually banning the peer for consistency.
                    this.add_edges(&clock, vec![edge.clone()])
                        .await
                        .map_err(|_: ReasonForBan| RegisterPeerError::InvalidEdge)?;
                    // Insert to the local connection pool
                    this.tier2.insert_ready(conn.clone()).map_err(RegisterPeerError::PoolError)?;
                    // Update the V2 routing table
                    #[cfg(feature = "distance_vector_routing")]
                    this.update_routes(&clock, NetworkTopologyChange::PeerConnected(peer_info.id.clone(), edge.clone()))
                        .await.map_err(|_: ReasonForBan| RegisterPeerError::InvalidEdge)?;
                    // Write to the peer store
                    this.peer_store.peer_connected(&clock, peer_info);
                }
                tcp::Tier::T3 => {
                    if conn.peer_type == PeerType::Inbound {
                        // TODO(saketh): When a peer initiates a TIER3 connection it should be
                        // responding to a request sent previously by the local node. If we
                        // maintain some state about pending requests it would be possible to add
                        // an additional layer of security here and reject unexpected connections.
                    }
                    if !edge.verify() {
                        return Err(RegisterPeerError::InvalidEdge);
                    }
                    this.tier3.insert_ready(conn).map_err(RegisterPeerError::PoolError)?;
                }
            }
            Ok(())
        }).await.unwrap()
    }

    /// Removes the connection from the state.
    /// It is intentionally synchronous and expected to be called from PeerActor.stopping.
    /// If it was async, there would be a risk that the unregister will be cancelled before
    /// even starting.
    pub fn unregister(
        self: &Arc<Self>,
        clock: &time::Clock,
        conn: &Arc<connection::Connection>,
        _stream_id: tcp::StreamId,
        reason: ClosingReason,
    ) {
        let this = self.clone();
        let clock = clock.clone();
        let conn = conn.clone();
        self.spawn(async move {
            match conn.tier {
                tcp::Tier::T1 => this.tier1.remove(&conn),
                tcp::Tier::T2 => this.tier2.remove(&conn),
                tcp::Tier::T3 => this.tier3.remove(&conn),
            }

            // The rest of this function has to do with banning or routing,
            // which are applicable only for TIER2.
            if conn.tier != tcp::Tier::T2 {
                return;
            }

            let peer_id = conn.peer_info.id.clone();

            // If the last edge we have with this peer represent a connection addition, create the edge
            // update that represents the connection removal.
            if let Some(edge) = this.graph.load().local_edges.get(&peer_id) {
                if edge.edge_type() == EdgeState::Active {
                    let edge_update =
                        edge.remove_edge(this.config.node_id(), &this.config.node_key);
                    this.add_edges(&clock, vec![edge_update.clone()]).await.unwrap();
                }
            }

            // Update the V2 routing table
            #[cfg(feature = "distance_vector_routing")]
            this.update_routes(&clock, NetworkTopologyChange::PeerDisconnected(peer_id.clone()))
                .await
                .unwrap();

            // Save the fact that we are disconnecting to the PeerStore.
            let res = match reason {
                ClosingReason::Ban(ban_reason) => {
                    this.peer_store.peer_ban(&clock, &conn.peer_info.id, ban_reason)
                }
                _ => this.peer_store.peer_disconnected(&clock, &conn.peer_info.id),
            };
            if let Err(err) = res {
                tracing::debug!(target: "network", ?err, "Failed to save peer data");
            }

            // Save the fact that we are disconnecting to the ConnectionStore,
            // and push a reconnect attempt, if applicable
            if this.connection_store.connection_closed(&conn.peer_info, &conn.peer_type, &reason) {
                this.pending_reconnect.lock().push(conn.peer_info.clone());
            }

            #[cfg(test)]
            this.config.event_sink.send(
                crate::peer_manager::peer_manager_actor::Event::ConnectionClosed(
                    crate::peer::peer_actor::ConnectionClosedEvent {
                        stream_id: _stream_id,
                        reason,
                    },
                ),
            );
        });
    }

    /// Attempt to connect to the given peer until successful, up to max_attempts times
    pub async fn reconnect(
        self: &Arc<Self>,
        clock: time::Clock,
        peer_info: PeerInfo,
        max_attempts: usize,
    ) {
        let mut interval = time::Interval::new(clock.now(), RECONNECT_ATTEMPT_INTERVAL);
        for _attempt in 0..max_attempts {
            interval.tick(&clock).await;

            let result = async {
                let stream =
                    tcp::Stream::connect(&peer_info, tcp::Tier::T2, &self.config.socket_options)
                        .await
                        .context("tcp::Stream::connect()")?;
                PeerActor::spawn_and_handshake(clock.clone(), stream, None, self.clone())
                    .await
                    .context("PeerActor::spawn()")?;
                anyhow::Ok(())
            }
            .await;

            let succeeded = !result.is_err();

            if let Err(ref err) = result {
                tracing::info!(target:"network", err = format!("{:#}", err), "Failed to connect to {peer_info}");
            }

            // The peer may not be in the peer store; we try to record the connection attempt but
            // ignore any storage errors
            let _ = self.peer_store.peer_connection_attempt(&clock, &peer_info.id, result);

            if succeeded {
                return;
            }
        }
    }

    /// Determine if the given target is referring to us.
    pub fn message_for_me(&self, target: &PeerIdOrHash) -> bool {
        let my_peer_id = self.config.node_id();
        match target {
            PeerIdOrHash::PeerId(peer_id) => &my_peer_id == peer_id,
            PeerIdOrHash::Hash(hash) => self.compare_route_back(*hash, &my_peer_id),
        }
    }

    #[cfg(test)]
    pub fn send_ping(&self, clock: &time::Clock, tier: tcp::Tier, nonce: u64, target: PeerId) {
        let body = T2MessageBody::Ping(crate::network_protocol::Ping {
            nonce,
            source: self.config.node_id(),
        })
        .into();
        let msg = RawRoutedMessage { target: PeerIdOrHash::PeerId(target), body };
        self.send_message_to_peer(clock, tier, self.sign_message(clock, msg));
    }

    pub fn send_pong(&self, clock: &time::Clock, tier: tcp::Tier, nonce: u64, target: CryptoHash) {
        let body = T2MessageBody::Pong(crate::network_protocol::Pong {
            nonce,
            source: self.config.node_id(),
        })
        .into();
        let msg = RawRoutedMessage { target: PeerIdOrHash::Hash(target), body };
        self.send_message_to_peer(clock, tier, self.sign_message(clock, msg));
    }

    pub fn sign_message(&self, clock: &time::Clock, msg: RawRoutedMessage) -> Box<RoutedMessage> {
        Box::new(msg.sign(
            &self.config.node_key,
            self.config.routed_message_ttl,
            Some(clock.now_utc()),
        ))
    }

    /// Route signed message to target peer.
    /// Return whether the message is sent or not.
    pub fn send_message_to_peer(
        &self,
        clock: &time::Clock,
        tier: tcp::Tier,
        msg: Box<RoutedMessage>,
    ) -> bool {
        let my_peer_id = self.config.node_id();

        // Check if the message is for myself and don't try to send it in that case.
        if let PeerIdOrHash::PeerId(target) = msg.target() {
            if target == &my_peer_id {
                tracing::debug!(target: "network", account_id = ?self.config.validator.account_id(), ?my_peer_id, ?msg, "Drop signed message to myself");
                metrics::CONNECTED_TO_MYSELF.inc();
                return false;
            }
        }
        match tier {
            tcp::Tier::T1 => {
                let peer_id = match msg.target() {
                    // If a message is a response, we try to load the target from the route back
                    // cache.
                    PeerIdOrHash::Hash(hash) => {
                        match self.tier1_route_back.lock().remove(clock, &hash) {
                            Some(peer_id) => peer_id,
                            None => return false,
                        }
                    }
                    PeerIdOrHash::PeerId(peer_id) => peer_id.clone(),
                };
                return self.tier1.send_message(peer_id, Arc::new(PeerMessage::Routed(msg)));
            }
            tcp::Tier::T2 => {
                match self.tier2_find_route(&clock, msg.target()) {
                    Ok(peer_id) => {
                        // Remember if we expect a response for this message.
                        if *msg.author() == my_peer_id && msg.expect_response() {
                            tracing::trace!(target: "network", ?msg, "initiate route back");
                            self.tier2_route_back.lock().insert(clock, msg.hash(), my_peer_id);
                        }
                        return self
                            .tier2
                            .send_message(peer_id, Arc::new(PeerMessage::Routed(msg)));
                    }
                    Err(find_route_error) => {
                        // TODO(MarX, #1369): Message is dropped here. Define policy for this case.
                        metrics::MessageDropped::NoRouteFound.inc(msg.body());

                        tracing::debug!(target: "network",
                              account_id = ?self.config.validator.account_id(),
                              to = ?msg.target(),
                              reason = ?find_route_error,
                              known_peers = ?self.graph.routing_table.reachable_peers(),
                              msg = ?msg.body(),
                            "Drop signed message"
                        );
                        return false;
                    }
                }
            }
            tcp::Tier::T3 => {
                let peer_id = match msg.target() {
                    PeerIdOrHash::Hash(_) => {
                        // There is no route back cache for TIER3 as all connections are direct
                        debug_assert!(false);
                        return false;
                    }
                    PeerIdOrHash::PeerId(peer_id) => peer_id.clone(),
                };
                return self.tier3.send_message(peer_id, Arc::new(PeerMessage::Routed(msg)));
            }
        }
    }

    /// Send message to specific account.
    /// Return whether the message is sent or not.
    /// The message might be sent over TIER1 or TIER2 connection depending on the message type.
    pub fn send_message_to_account(
        self: &Arc<Self>,
        clock: &time::Clock,
        account_id: &AccountId,
        msg: TieredMessageBody,
    ) -> bool {
        // If the message is allowed to be sent to self, we handle it directly.
        if self.config.validator.account_id().is_some_and(|id| &id == account_id) {
            // For now, we don't allow some types of messages to be sent to self.
            debug_assert!(msg.allow_sending_to_self());
            let this = self.clone();
            let clock = clock.clone();
            let my_peer_id = self.config.node_id();
            let msg = self.sign_message(
                &clock,
                RawRoutedMessage { target: PeerIdOrHash::PeerId(my_peer_id.clone()), body: msg },
            );
            actix::spawn(async move {
                let hash = msg.hash();
                this.receive_routed_message(
                    &clock,
                    msg.author().clone(),
                    my_peer_id,
                    hash,
                    msg.body_owned(),
                )
                .await;
            });
            return true;
        }

        let accounts_data = self.accounts_data.load();
        if tcp::Tier::T1.is_allowed_routed(&msg) {
            for key in accounts_data.keys_by_id.get(account_id).iter().flat_map(|keys| keys.iter())
            {
                let data = match accounts_data.data.get(key) {
                    Some(data) => data,
                    None => continue,
                };
                let conn = match self.get_tier1_proxy(data) {
                    Some(conn) => conn,
                    None => continue,
                };
                // TODO(gprusak): in case of PartialEncodedChunk, consider stripping everything
                // but the header. This will bound the message size
                conn.send_message(Arc::new(PeerMessage::Routed(self.sign_message(
                    clock,
                    RawRoutedMessage {
                        target: PeerIdOrHash::PeerId(data.peer_id.clone()),
                        body: msg,
                    },
                ))));
                return true;
            }
        }

        let peer_id_from_account_data = accounts_data
            .keys_by_id
            .get(account_id)
            .iter()
            .flat_map(|keys| keys.iter())
            .find_map(|key| accounts_data.data.get(key))
            .map(|data| data.peer_id.clone());
        // Find the target peer_id:
        // - first look it up in self.accounts_data
        // - if missing, fall back to lookup in self.graph.routing_table
        // We want to deprecate self.graph.routing_table.account_owner in the next release.
        let target = if let Some(peer_id) = peer_id_from_account_data {
            metrics::ACCOUNT_TO_PEER_LOOKUPS.with_label_values(&["AccountData"]).inc();
            peer_id
        } else if let Some(peer_id) = self.account_announcements.get_account_owner(account_id) {
            metrics::ACCOUNT_TO_PEER_LOOKUPS.with_label_values(&["AnnounceAccount"]).inc();
            peer_id
        } else {
            // TODO(MarX, #1369): Message is dropped here. Define policy for this case.
            metrics::MessageDropped::UnknownAccount.inc(&msg);
            tracing::debug!(target: "network",
                   account_id = ?self.config.validator.account_id(),
                   to = ?account_id,
                   ?msg,"Drop message: unknown account",
            );
            tracing::trace!(target: "network", known_peers = ?self.account_announcements.get_accounts_keys(), "Known peers");
            return false;
        };

        let mut success = false;
        let msg = RawRoutedMessage { target: PeerIdOrHash::PeerId(target), body: msg };
        let msg = self.sign_message(clock, msg);
        for _ in 0..msg.body().message_resend_count() {
            success |= self.send_message_to_peer(clock, tcp::Tier::T2, msg.clone());
        }
        success
    }

    pub async fn receive_routed_message(
        self: &Arc<Self>,
        clock: &time::Clock,
        msg_author: PeerId,
        prev_hop: PeerId,
        msg_hash: CryptoHash,
        body: TieredMessageBody,
    ) -> Option<TieredMessageBody> {
        match body {
            TieredMessageBody::T1(body) => match *body {
                T1MessageBody::BlockApproval(approval) => {
                    self.client
                        .send_async(BlockApproval(approval, prev_hop).span_wrap())
                        .await
                        .ok();
                    None
                }
                T1MessageBody::VersionedPartialEncodedChunk(chunk) => {
                    self.shards_manager_adapter
                        .send(ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunk(*chunk));
                    None
                }
                T1MessageBody::PartialEncodedChunkForward(msg) => {
                    self.shards_manager_adapter.send(
                        ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunkForward(msg),
                    );
                    None
                }
                T1MessageBody::PartialEncodedStateWitness(witness) => {
                    self.partial_witness_adapter.send(PartialEncodedStateWitnessMessage(witness));
                    None
                }
                T1MessageBody::PartialEncodedStateWitnessForward(witness) => {
                    self.partial_witness_adapter
                        .send(PartialEncodedStateWitnessForwardMessage(witness));
                    None
                }
                T1MessageBody::VersionedChunkEndorsement(endorsement) => {
                    self.client.send_async(ChunkEndorsementMessage(endorsement)).await.ok();
                    None
                }
                T1MessageBody::ChunkContractAccesses(accesses) => {
                    self.partial_witness_adapter.send(ChunkContractAccessesMessage(accesses));
                    None
                }
                T1MessageBody::ContractCodeRequest(request) => {
                    self.partial_witness_adapter.send(ContractCodeRequestMessage(request));
                    None
                }
                T1MessageBody::ContractCodeResponse(response) => {
                    self.partial_witness_adapter.send(ContractCodeResponseMessage(response));
                    None
                }
            },
            TieredMessageBody::T2(body) => match *body {
                T2MessageBody::TxStatusRequest(account_id, tx_hash) => self
                    .client
                    .send_async(TxStatusRequest { tx_hash, signer_account_id: account_id })
                    .await
                    .ok()
                    .flatten()
                    .map(|response| {
                        TieredMessageBody::T2(Box::new(T2MessageBody::TxStatusResponse(response)))
                    }),
                T2MessageBody::TxStatusResponse(tx_result) => {
                    self.client.send_async(TxStatusResponse(tx_result.into())).await.ok();
                    None
                }
                T2MessageBody::ForwardTx(transaction) => {
                    self.client
                        .send_async(ProcessTxRequest {
                            transaction,
                            is_forwarded: true,
                            check_only: false,
                        })
                        .await
                        .ok();
                    None
                }
                T2MessageBody::PartialEncodedChunkRequest(request) => {
                    self.shards_manager_adapter.send(
                        ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunkRequest {
                            partial_encoded_chunk_request: request,
                            route_back: msg_hash,
                        },
                    );
                    None
                }
                T2MessageBody::PartialEncodedChunkResponse(response) => {
                    self.shards_manager_adapter.send(
                        ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunkResponse {
                            partial_encoded_chunk_response: response,
                            received_time: clock.now().into(),
                        },
                    );
                    None
                }
                T2MessageBody::ChunkStateWitnessAck(ack) => {
                    self.partial_witness_adapter.send(ChunkStateWitnessAckMessage(ack));
                    None
                }
                T2MessageBody::StateHeaderRequest(request) => {
                    self.peer_manager_adapter.send(Tier3Request {
                        peer_info: PeerInfo {
                            id: msg_author,
                            addr: Some(request.addr),
                            account_id: None,
                        },
                        body: Tier3RequestBody::StateHeader(StateHeaderRequestBody {
                            shard_id: request.shard_id,
                            sync_hash: request.sync_hash,
                        }),
                    });
                    None
                }
                T2MessageBody::StatePartRequest(request) => {
                    self.peer_manager_adapter.send(Tier3Request {
                        peer_info: PeerInfo {
                            id: msg_author,
                            addr: Some(request.addr),
                            account_id: None,
                        },
                        body: Tier3RequestBody::StatePart(StatePartRequestBody {
                            shard_id: request.shard_id,
                            sync_hash: request.sync_hash,
                            part_id: request.part_id,
                        }),
                    });
                    None
                }
                T2MessageBody::PartialEncodedContractDeploys(deploys) => {
                    self.partial_witness_adapter
                        .send(PartialEncodedContractDeploysMessage(deploys));
                    None
                }
                body => {
                    tracing::error!(target: "network", "Peer received unexpected message type: {:?}", body);
                    None
                }
            },
        }
    }

    pub async fn add_accounts_data(
        self: &Arc<Self>,
        clock: &time::Clock,
        accounts_data: Vec<Arc<SignedAccountData>>,
    ) -> Option<AccountDataError> {
        let this = self.clone();
        let clock = clock.clone();
        self.spawn(async move {
            // Verify and add the new data to the internal state.
            let (new_data, err) = this.accounts_data.clone().insert(&clock, accounts_data).await;
            // Broadcast any new data we have found, even in presence of an error.
            // This will prevent a malicious peer from forcing us to re-verify valid
            // datasets. See accounts_data::Cache documentation for details.
            if !new_data.is_empty() {
                let tier2 = this.tier2.load();
                let tasks: Vec<_> = tier2
                    .ready
                    .values()
                    .map(|p| this.spawn(p.send_accounts_data(new_data.clone())))
                    .collect();
                for t in tasks {
                    t.await.unwrap();
                }
            }
            err
        })
        .await
        .unwrap()
    }

    pub async fn add_snapshot_hosts(
        self: &Arc<Self>,
        hosts: Vec<Arc<SnapshotHostInfo>>,
    ) -> Option<SnapshotHostInfoError> {
        let this = self.clone();
        self.spawn(async move {
            // Verify and add the new data to the internal state.
            let (new_data, err) = this.snapshot_hosts.clone().insert(hosts).await;
            // Broadcast any valid new data, even if an err was returned.
            // The presence of one invalid entry doesn't invalidate the remaining ones.
            if !new_data.is_empty() {
                let tier2 = this.tier2.load();
                let tasks: Vec<_> = tier2
                    .ready
                    .values()
                    .map(|p| this.spawn(p.send_snapshot_hosts(new_data.clone())))
                    .collect();
                for t in tasks {
                    t.await.unwrap();
                }
            }
            err
        })
        .await
        .unwrap()
    }

    /// a) there is a peer we should be connected to, but we aren't
    /// b) there is an edge indicating that we should be disconnected from a peer, but we are connected.
    /// Try to resolve the inconsistency.
    /// We call this function every FIX_LOCAL_EDGES_INTERVAL from peer_manager_actor.rs.
    pub async fn fix_local_edges(self: &Arc<Self>, clock: &time::Clock, timeout: time::Duration) {
        let this = self.clone();
        let clock = clock.clone();
        self.spawn(async move {
            let graph = this.graph.load();
            let tier2 = this.tier2.load();
            let mut tasks = vec![];
            for edge in graph.local_edges.values() {
                let edge = edge.clone();
                let node_id = this.config.node_id();
                let other_peer = edge.other(&node_id).unwrap();
                match (tier2.ready.get(other_peer), edge.edge_type()) {
                    // This is an active connection, while the edge indicates it shouldn't.
                    (Some(conn), EdgeState::Removed) => tasks.push(this.spawn({
                        let this = this.clone();
                        let conn = conn.clone();
                        let clock = clock.clone();
                        async move {
                            conn.send_message(Arc::new(PeerMessage::RequestUpdateNonce(
                                PartialEdgeInfo::new(
                                    &node_id,
                                    &conn.peer_info.id,
                                    std::cmp::max(Edge::create_fresh_nonce(&clock), edge.next()),
                                    &this.config.node_key,
                                ),
                            )));
                            // TODO(gprusak): here we should synchronically wait for the RequestUpdateNonce
                            // response (with timeout). Until network round trips are implemented, we just
                            // blindly wait for a while, then check again.
                            clock.sleep(timeout).await;
                            match this.graph.load().local_edges.get(&conn.peer_info.id) {
                                Some(edge) if edge.edge_type() == EdgeState::Active => return,
                                _ => conn.stop(None),
                            }
                        }
                    })),
                    // We are not connected to this peer, but routing table contains
                    // information that we do. We should wait and remove that peer
                    // from routing table
                    (None, EdgeState::Active) => tasks.push(this.spawn({
                        let this = this.clone();
                        let clock = clock.clone();
                        let other_peer = other_peer.clone();
                        async move {
                            // This edge says this is an connected peer, which is currently not in the set of connected peers.
                            // Wait for some time to let the connection begin or broadcast edge removal instead.
                            clock.sleep(timeout).await;
                            if this.tier2.load().ready.contains_key(&other_peer) {
                                return;
                            }
                            // Peer is still not connected after waiting a timeout.
                            // Unwrap is safe, because new_edge is always valid.
                            let new_edge =
                                edge.remove_edge(this.config.node_id(), &this.config.node_key);
                            this.add_edges(&clock, vec![new_edge.clone()]).await.unwrap()
                        }
                    })),
                    // OK
                    _ => {}
                }
            }
            for t in tasks {
                let _ = t.await;
            }

            // Now that `graph` has been synchronized with the state of the local connections,
            // use it as the source of truth to fix the local state in `graph_v2`
            #[cfg(feature = "distance_vector_routing")]
            {
                let mut tasks = vec![];
                let node_id = this.config.node_id();
                for edge in graph.local_edges.values() {
                    let other_peer = edge.other(&node_id).unwrap();
                    tasks.push(match edge.edge_type() {
                        EdgeState::Active => this.update_routes(
                            &clock,
                            NetworkTopologyChange::PeerConnected(other_peer.clone(), edge.clone()),
                        ),
                        EdgeState::Removed => this.update_routes(
                            &clock,
                            NetworkTopologyChange::PeerDisconnected(other_peer.clone()),
                        ),
                    });
                }
                for t in tasks {
                    let _ = t.await;
                }
            }
        })
        .await
        .unwrap()
    }

    pub fn update_connection_store(self: &Arc<Self>, clock: &time::Clock) {
        self.connection_store.update(clock, &self.tier2.load());
    }

    /// Clears pending_reconnect and returns the cleared values
    pub fn poll_pending_reconnect(&self) -> Vec<PeerInfo> {
        let mut pending_reconnect = self.pending_reconnect.lock();
        let polled = pending_reconnect.clone();
        pending_reconnect.clear();
        return polled;
    }

    /// Collects and returns PeerInfos for all directly connected TIER2 peers.
    pub fn get_direct_peers(self: &Arc<Self>) -> Vec<PeerInfo> {
        return self.tier2.load().ready.values().map(|c| c.peer_info.clone()).collect();
    }

    /// Sets the chain info, and updates the set of TIER1 keys.
    /// Returns true iff the set of TIER1 keys has changed.
    pub fn set_chain_info(self: &Arc<Self>, info: ChainInfo) -> bool {
        let _mutex = self.set_chain_info_mutex.lock();

        // We set state.chain_info and call accounts_data.set_keys
        // synchronously, therefore, assuming actix in-order delivery, there
        // will be no race condition between subsequent SetChainInfo calls.
        self.chain_info.store(Arc::new(Some(info.clone())));

        // The set of TIER1 accounts has changed, so we might be missing some
        // accounts_data that our peers know about.
        let has_changed = self.accounts_data.set_keys(info.tier1_accounts);
        if has_changed {
            self.tier1_request_full_sync();
        }
        has_changed
    }
}
