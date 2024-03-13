package com.order.processor.dao.repository;

import com.order.processor.dao.entity.Order;
import com.order.processor.dao.entity.OrderDetail;
import com.order.processor.dao.entity.User;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface OrderDetailRepository extends JpaRepository<OrderDetail, Long> {
}
