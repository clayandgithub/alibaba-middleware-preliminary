package com.alibaba.middleware.race.model;

import java.io.Serializable;

/**
 * ���Ǻ�̨RocketMq�洢�Ķ�����Ϣģ��������OrderMessage��ѡ��Ҳ�����Զ���
 * ������Ϣģ�ͣ�ֻҪģ���и����ֶε����ͺ�˳���OrderMessageһ����������Kryo
 * �����г���Ϣ
 */
public class OrderMessage implements Serializable{
    private static final long serialVersionUID = -4082657304129211564L;
    private long orderId; 
    private String buyerId; 
    private String productId; 

    private String salerId; 
    private long createTime; 
    private double totalPrice;


    private OrderMessage() {

    }

    public static OrderMessage createTbaoMessage() {
        OrderMessage msg =  new OrderMessage();
        msg.orderId = TableItemFactory.createOrderId();
        msg.buyerId = TableItemFactory.createBuyerId();
        msg.productId = TableItemFactory.createProductId();
        msg.salerId = TableItemFactory.createTbaoSalerId();
        msg.totalPrice = TableItemFactory.createTotalPrice();
        return msg;
    }

    public static OrderMessage createTmallMessage() {
        OrderMessage msg =  new OrderMessage();
        msg.orderId = TableItemFactory.createOrderId();
        msg.buyerId = TableItemFactory.createBuyerId();
        msg.productId = TableItemFactory.createProductId();
        msg.salerId = TableItemFactory.createTmallSalerId();
        msg.totalPrice = TableItemFactory.createTotalPrice();

        return msg;
    }

    @Override
    public String toString() {
        return "OrderMessage{" +
                "orderId=" + orderId +
                ", buyerId='" + buyerId + '\'' +
                ", productId='" + productId + '\'' +
                ", salerId='" + salerId + '\'' +
                ", createTime=" + createTime +
                ", totalPrice=" + totalPrice +
                '}';
    }

    public double getTotalPrice() {
        return totalPrice;
    }

    public void setTotalPrice(double totalPrice) {
        this.totalPrice = totalPrice;
    }

    public long getOrderId() {
        return orderId;
    }

    public void setOrderId(long orderId) {
        this.orderId = orderId;
    }

    public String getBuyerId() {
        return buyerId;
    }

    public void setBuyerId(String buyerId) {
        this.buyerId = buyerId;
    }


    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }


    public String getSalerId() {
        return salerId;
    }

    public void setSalerId(String salerId) {
        this.salerId = salerId;
    }

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

}
