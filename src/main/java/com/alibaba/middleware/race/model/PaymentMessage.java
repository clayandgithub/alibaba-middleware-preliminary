package com.alibaba.middleware.race.model;

import java.io.Serializable;
import java.util.Random;

public class PaymentMessage implements Serializable{

    private static final long serialVersionUID = -4721410670774102273L;

    private long orderId; 

    private double payAmount; 


    private short paySource; 


    private short payPlatform; 


    private long createTime;

    public PaymentMessage() {
    }
    
    public PaymentMessage(long orderID, double payAmount, short paySource, short payPlatform, long createTime){
    	this.orderId = orderID;
    	this.payAmount = payAmount;
    	this.paySource = paySource;
    	this.payPlatform = payPlatform;
    	this.createTime = createTime;
    }

    private static Random rand = new Random();

    public static PaymentMessage[] createPayMentMsg(OrderMessage orderMessage) {
        PaymentMessage [] list = new PaymentMessage[2];
        for (short i = 0; i < 2; i++) {
            PaymentMessage msg = new PaymentMessage();
            msg.orderId = orderMessage.getOrderId();
            msg.paySource = i;
            msg.payPlatform = (short) (i % 2);
            msg.createTime = orderMessage.getCreateTime() + rand.nextInt(100);
            msg.payAmount = 0.0;
            list[i] = msg;
        }

        list[0].payAmount = rand.nextInt((int) (orderMessage.getTotalPrice() / 2));
        list[1].payAmount = orderMessage.getTotalPrice() - list[0].payAmount;

        return list;
    }

    @Override
    public String toString() {
        return "PaymentMessage{" +
                "orderId=" + orderId +
                ", payAmount=" + payAmount +
                ", paySource=" + paySource +
                ", payPlatform=" + payPlatform +
                ", createTime=" + createTime +
                '}';
    }

    public long getOrderId() {
        return orderId;
    }

    public void setOrderId(long orderId) {
        this.orderId = orderId;
    }

    public double getPayAmount() {
        return payAmount;
    }

    public void setPayAmount(double payAmount) {
        this.payAmount = payAmount;
    }

    public short getPaySource() {
        return paySource;
    }

    public void setPaySource(short paySource) {
        this.paySource = paySource;
    }

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    public short getPayPlatform() {
        return payPlatform;
    }
    
    public long getUniqueToken() {
        return orderId + createTime + (long)payAmount + paySource * 100 + payPlatform * 10000 + payPlatform * 1000000;
    }
}
