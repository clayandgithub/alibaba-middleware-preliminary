package com.alibaba.middleware.race.model;

import java.io.Serializable;

import com.alibaba.middleware.race.Constants;

public class PaymentMessageExt implements Serializable {

    private static final long serialVersionUID = 4287689919215539708L;

    public static final short UNSOLVED_PLATFORM = -1;

    private long orderId; 

    private double payAmount; 

    private short payPlatform; 

    private long createTime;
    
    private short salerPlatform = UNSOLVED_PLATFORM;

    public PaymentMessageExt(PaymentMessage paymentMessage) {
        orderId = paymentMessage.getOrderId();
        payAmount = paymentMessage.getPayAmount();
        payPlatform = paymentMessage.getPayPlatform();
        createTime = paymentMessage.getCreateTime();
    }
    
    public PaymentMessageExt(){
    	
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

    public short getPayPlatform() {
        return payPlatform;
    }

    public void setPayPlatform(short payPlatform) {
        this.payPlatform = payPlatform;
    }

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    public short getSalerPlatform() {
        return salerPlatform;
    }

    public void setSalerPlatform(short salerPlatform) {
        this.salerPlatform = salerPlatform;
    }

    public boolean isSalerPlatformSolved() {
        return salerPlatform != UNSOLVED_PLATFORM;
    }
    
    public boolean isSalerPlatformTB() {
        return salerPlatform == Constants.TAOBAO;
    }
}
