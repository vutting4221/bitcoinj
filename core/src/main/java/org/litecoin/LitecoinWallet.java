package org.litecoin;

import com.google.bitcoin.core.*;
import com.google.bitcoin.crypto.KeyCrypter;

import java.math.BigInteger;

/**
 * An extension of BitcoinJ's wallet to use Litecoin's fee structure by default
 */
public class LitecoinWallet extends Wallet {
    public LitecoinWallet(NetworkParameters params) {
        super(params);
    }

    public LitecoinWallet(NetworkParameters params, KeyCrypter keyCrypter) {
        super(params, keyCrypter);
    }

    /** Transactions which give us less than this value are ignored, this is not ideal as we can still get spammed with
      * tons of small outputs in a single transaction, but at that point we received actual money, so...oh well */
    public BigInteger MIN_VALUE = Utils.CENT.divide(BigInteger.valueOf(100));

    @Override
    public boolean isTransactionRelevant(Transaction tx) {
        return super.isTransactionRelevant(tx) && (tx.getValueSentToMe(this).compareTo(MIN_VALUE) >= 0 ||
                tx.getValueSentFromMe(this).compareTo(BigInteger.ZERO) > 0);
    }

    public boolean isPendingTransactionRelevant(Transaction tx) throws ScriptException {
        return super.isPendingTransactionRelevant(tx) && (tx.getValueSentToMe(this).compareTo(MIN_VALUE) >= 0 ||
                tx.getValueSentFromMe(this).compareTo(BigInteger.ZERO) > 0);
    }

    // Override Send stuff to set the right fee params
    private void setRequestFeeForOutputs(SendRequest req) {
        req.ensureMinRequiredFee = false;
        req.fee = Utils.CENT.multiply(BigInteger.valueOf(2));
        for (TransactionOutput output : req.tx.getOutputs())
            if (output.getValue().compareTo(Utils.CENT) < 0)
                req.fee = req.fee.add(Utils.CENT.multiply(BigInteger.valueOf(2)));
    }

    public Transaction createSend(Address address, BigInteger nanocoins) throws InsufficientMoneyException {
        SendRequest req = SendRequest.to(address, nanocoins);
        setRequestFeeForOutputs(req);
        completeTx(req);
        return req.tx;
    }

    public Transaction sendCoinsOffline(SendRequest request) throws InsufficientMoneyException {
        setRequestFeeForOutputs(request);
        return super.sendCoinsOffline(request);
    }

    public SendResult sendCoins(TransactionBroadcaster broadcaster, Address to, BigInteger value) throws InsufficientMoneyException {
        SendRequest request = SendRequest.to(to, value);
        setRequestFeeForOutputs(request);
        return sendCoins(broadcaster, request);
    }

    public SendResult sendCoins(TransactionBroadcaster broadcaster, SendRequest request) throws InsufficientMoneyException {
        setRequestFeeForOutputs(request);
        return super.sendCoins(broadcaster, request);
    }

    public SendResult sendCoins(SendRequest request) throws InsufficientMoneyException {
        setRequestFeeForOutputs(request);
        return super.sendCoins(request);
    }

    public Transaction sendCoins(Peer peer, SendRequest request) throws InsufficientMoneyException {
        setRequestFeeForOutputs(request);
        return super.sendCoins(peer, request);
    }

    public void completeTx(SendRequest req) throws InsufficientMoneyException {
        setRequestFeeForOutputs(req);
        super.completeTx(req);
    }
}
