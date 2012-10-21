package com.google.bitcoin.examples;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import com.google.bitcoin.core.AbstractPeerEventListener;
import com.google.bitcoin.core.Block;
import com.google.bitcoin.core.BlockChain;
import com.google.bitcoin.core.FullPrunedBlockChain;
import com.google.bitcoin.core.GetDataMessage;
import com.google.bitcoin.core.InventoryItem;
import com.google.bitcoin.core.InventoryMessage;
import com.google.bitcoin.core.Message;
import com.google.bitcoin.core.NetworkParameters;
import com.google.bitcoin.core.Peer;
import com.google.bitcoin.core.PeerGroup;
import com.google.bitcoin.core.PrunedException;
import com.google.bitcoin.core.Sha256Hash;
import com.google.bitcoin.core.StoredBlock;
import com.google.bitcoin.core.VerificationException;
import com.google.bitcoin.discovery.DnsDiscovery;
import com.google.bitcoin.store.FullPrunedBlockStore;
import com.google.bitcoin.store.H2FullPrunedBlockStore;

/**
 * This class demonstrates how to use a FullPrunedBlockStore to do a two-phase block chain download:
 *      1. Download headers up to the current block chain tip.
 *      2. Fill in full blocks, spreading the download burden between peers and 
 * TODO: This is quite a hack and a lot of code needs to move to PeerGroup/*BlockChain
 */
public class TwoPhaseDownload {
    static int peerTimeout = 10;
    static int blockAddExecutorQueueDepth = 0;
    static int blocksRequired = 0;
    public static void main(String[] args) throws Exception {
        final NetworkParameters params = NetworkParameters.prodNet();
        FullPrunedBlockStore store = new H2FullPrunedBlockStore(params, "/home/matt/.bitcoin/h2-full", 1000);
        // Attach wallets to chain, not fullChain
        final BlockChain chain = new BlockChain(params, store);
        
        PeerGroup peerGroup = new PeerGroup(params, chain);
        // Obviously dont do this if you have wallets which need filling (or do, and be smart...)
        peerGroup.setFastCatchupTimeSecs(Long.MAX_VALUE);
        peerGroup.setUserAgent("TwoPhaseDownloadExample", "0.1");
        peerGroup.addPeerDiscovery(new DnsDiscovery(params));
        
        peerGroup.connectTo(new InetSocketAddress(InetAddress.getByName("dorm.bluematt.me"), 8333));
        peerGroup.start();
        peerGroup.downloadBlockChain();
        
        // First phase (headers-only) done, close the peer group
        peerGroup.stop();
        while (peerGroup.numConnectedPeers() > 0)
            Thread.sleep(100);
        
        final FullPrunedBlockChain fullChain = new FullPrunedBlockChain(params, store);
        final PeerGroup fullPeerGroup = new PeerGroup(params, fullChain);
        fullPeerGroup.setUserAgent("TwoPhaseDownloadExample", "0.1");
        fullPeerGroup.addPeerDiscovery(new DnsDiscovery(params));
        
        Sha256Hash targetBlockHash = store.getVerifiedChainHead().getHeader().getHash();
        StoredBlock tempBlock = store.getChainHead();
        Sha256Hash tempBlockHash = tempBlock.getHeader().getHash();
        final LinkedList<Sha256Hash> blocksToDownload = new LinkedList<Sha256Hash>();
        while (tempBlockHash != null && !tempBlockHash.equals(targetBlockHash)) {
            blocksToDownload.addFirst(tempBlockHash);
            tempBlock = tempBlock.getPrev(store);
            tempBlockHash = tempBlock.getHeader().getHash();
        }
        
        final Sha256Hash downloadTargetBlock = chain.getChainHead().getHeader().getHash();
        
        final Semaphore doneDownloading = new Semaphore(0);
        
        final ScheduledThreadPoolExecutor peerDownloadTimeout = new ScheduledThreadPoolExecutor(1);
        final List<Peer> timedOutPeers = new LinkedList<Peer>();
        final HashMap<Peer, LinkedHashSet<Sha256Hash>> pendingGetDataMessages = new HashMap<Peer, LinkedHashSet<Sha256Hash>>();
        
        final ExecutorService blockAddExecutor = Executors.newFixedThreadPool(1);
        final LinkedList<Peer> peersWaitingOnBlockChain = new LinkedList<Peer>();
        fullPeerGroup.addEventListener(new AbstractPeerEventListener() {
            private void readdBlocksToDownloadPool(LinkedHashSet<Sha256Hash> hashSetToDownload) {
                Object[] hashList = hashSetToDownload.toArray();
                synchronized (blocksToDownload) {
                    for (int i = hashList.length-1; i > -1; i--)
                        blocksToDownload.addFirst((Sha256Hash)hashList[i]);
                    blocksRequired += hashList.length;
                }
                boolean retry = true;
                while (retry) {
                    retry = false;
                    Peer peerToAskForBlocks = null;
                    synchronized (peersWaitingOnBlockChain) {
                        peerToAskForBlocks = peersWaitingOnBlockChain.pollFirst();
                    }
                    if (peerToAskForBlocks != null) {
                        try {
                            askPeerForBlocks(peerToAskForBlocks);
                        } catch (IOException e) {
                            retry = true;
                        }
                    }
                }
            }
            
            private void asyncProcessBlock(final Block block) {
                synchronized(peersWaitingOnBlockChain) {
                    blockAddExecutorQueueDepth++;
                }
                blockAddExecutor.submit(new Runnable() {
                    public void run() {
                        // We can count the current block as an "orphan" because it is not yet connected
                        // and we don't want to add one if this block is just an orphan
                        int initialOrphanCount = fullChain.getOrphanCount() + 1;
                        try {
                            fullChain.add(block);
                        } catch (VerificationException e) {
                            // Note that this is not thrown to the Peer in a message handler:
                            // Its not this peer's fault - reference clients will gladly serve us
                            // invalid blocks if we ask for them by hash, its really the fault of the
                            // peer we originally synced with (in the first phase)
                            try {
                                chain.forceReorgToHash(fullChain.getChainHead().getHeader().getHash());
                            } catch (Exception e1) {
                                // TODO: Freak out more than just killing the async block add thread
                                throw new RuntimeException(e1);
                            }
                        } catch (PrunedException e) {
                            //TODO: Handle this case
                        }
                        StoredBlock newChainHead = fullChain.getChainHead();
                        if (newChainHead.getHeader().getHash().equals(downloadTargetBlock))
                            doneDownloading.release();
                        if (newChainHead.getHeight() % 5000 == 0)
                            System.out.println("Block chain is now at height: " + newChainHead.getHeight());
                        Peer peerToAskForBlocks = null;
                        synchronized(peersWaitingOnBlockChain) {
                            blockAddExecutorQueueDepth += fullChain.getOrphanCount() - initialOrphanCount;
                            if (blockAddExecutorQueueDepth < 500)
                                peerToAskForBlocks = peersWaitingOnBlockChain.pollFirst();
                        }
                        if (peerToAskForBlocks != null) {
                            try {
                                askPeerForBlocks(peerToAskForBlocks);
                            } catch (IOException e) {
                                // Swallow exceptions and let this peer die naturally
                            }
                        }
                    }
                });
            }
            
            private void askPeerForBlocks(final Peer peer) throws IOException {
                boolean skipWaitCheck = false;
                synchronized(blocksToDownload) {
                    if (blocksRequired > 0)
                        skipWaitCheck = true;
                }
                
                synchronized(peersWaitingOnBlockChain) {
                    // Only queue up to 500 blocks, after that, back off and
                    // just add this peer to the end of the queue to get asked for blocks when we need more
                    // Note that we will still have consistently more than 100 blocks in the queue
                    if (blockAddExecutorQueueDepth >= 500) {
                        if (!skipWaitCheck) {
                            peersWaitingOnBlockChain.addLast(peer);
                            return;
                        } else
                            System.out.println("Forcing askPeerForBlocks due to blocksRequired > 0");
                    }
                }
                
                // The message to send the peer
                final GetDataMessage getBlocksMessage = new GetDataMessage(params);
                // A set to keep track of blocks which have been requested from a given peer
                final LinkedHashSet<Sha256Hash> hashSetToDownload = new LinkedHashSet<Sha256Hash>();
                synchronized (blocksToDownload) {
                    // Ask for 100 blocks at a time so that we don't spend too much time waiting around
                    for (int i = 0; i < 100; i++) {
                        Sha256Hash nextBlockToDownload = blocksToDownload.pollFirst();
                        if (nextBlockToDownload != null) {
                            getBlocksMessage.addItem(new InventoryItem(InventoryItem.Type.Block, nextBlockToDownload));
                            hashSetToDownload.add(nextBlockToDownload);
                        } else
                            break;
                    }
                    if (skipWaitCheck) {
                        blocksRequired -= hashSetToDownload.size();
                        if (blocksRequired < 0)
                            blocksRequired = 0;
                    }
                }
                if (hashSetToDownload.size() != 0) {
                    System.out.println("Asking " + peer.toString() + " for " + hashSetToDownload.size() + " blocks");
                    try {
                        peer.sendMessage(getBlocksMessage);
                    } catch (IOException e) {
                        readdBlocksToDownloadPool(hashSetToDownload);
                        throw e;
                    }
                    synchronized(pendingGetDataMessages) {
                        pendingGetDataMessages.put(peer, hashSetToDownload);
                    }
                    // Schedule the timeout Runnable
                    peerDownloadTimeout.schedule(new Runnable() {
                        public void run() {
                            boolean timedOut = false;
                            List<Peer> peersToAskForBlocks = null;
                            synchronized(pendingGetDataMessages) {
                                LinkedHashSet<Sha256Hash> currentSet = pendingGetDataMessages.get(peer);
                                if (currentSet == hashSetToDownload) {
                                    synchronized(timedOutPeers) {
                                        // If more than half of our peers have timed out,
                                        // exponentially increase the timeout and re-add timed out peers
                                        if (timedOutPeers.size() >= fullPeerGroup.numConnectedPeers() / 2) {
                                            peersToAskForBlocks = new LinkedList<Peer>(timedOutPeers);
                                            timedOutPeers.clear();
                                            peerTimeout *= 2;
                                            System.out.println("Peers aren't returning blocks fast enough (probably our fault) - increasing timeout interval");
                                        } else {
                                            timedOut = true;
                                            timedOutPeers.add(peer);
                                        }
                                    }
                                    if (timedOut)
                                        pendingGetDataMessages.remove(peer);
                                }
                            }
                            if (timedOut) {
                                System.out.println(peer.toString() + " timed out requesting blocks");
                                readdBlocksToDownloadPool(hashSetToDownload);
                            }
                            if (peersToAskForBlocks != null)
                                for (Peer peer : peersToAskForBlocks) {
                                    try {
                                        askPeerForBlocks(peer);
                                    } catch (IOException e) {
                                        // Swallow exceptions and let this peer die naturally
                                    }
                                }
                        }
                    }, peerTimeout, TimeUnit.SECONDS);
                }
            }
            
            @Override
            public void onPeerConnected(Peer peer, int peerCount) {
                try {
                    askPeerForBlocks(peer);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            
            @Override
            public void onPeerDisconnected(Peer peer, int peerCount) {
                LinkedHashSet<Sha256Hash> blocksStillToDownload;
                synchronized(pendingGetDataMessages) {
                    blocksStillToDownload = pendingGetDataMessages.remove(peer);
                }
                if (blocksStillToDownload != null)
                    readdBlocksToDownloadPool(blocksStillToDownload);
            }
            
            @Override
            public Message onPreMessageReceived(Peer peer, final Message m) {
                if (m instanceof Block) {
                    boolean askForBlocks = false;
                    synchronized(pendingGetDataMessages) {
                        LinkedHashSet<Sha256Hash> blocksStillToDownload = pendingGetDataMessages.get(peer);
                        if (blocksStillToDownload == null)
                            return null; // We didn't ask this peer for any blocks
                        if (!blocksStillToDownload.remove(((Block)m).getHash()))
                            return null; // We didn't ask for this block
                        if (blocksStillToDownload.size() == 0) {
                            askForBlocks = true;
                            pendingGetDataMessages.remove(peer);
                        }
                    }
                    // Process the block asynchronously to make timeouts useful
                    asyncProcessBlock((Block)m);
                    if (askForBlocks) {
                        System.out.println(peer.toString() + " returned all the requested blocks");
                        try {
                            askPeerForBlocks(peer);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    return null;
                } else if (m instanceof InventoryMessage)
                    return null;
                return m;
            }
        });
        System.out.println("Starting second phase of blockchain download...");
        fullPeerGroup.connectTo(new InetSocketAddress(InetAddress.getByName("dorm.bluematt.me"), 8333));
        fullPeerGroup.start();
        doneDownloading.acquire();
        System.out.println("Done with second phase of blockchain download");
        fullPeerGroup.stop();
        Thread.sleep(1000);
    }
}
