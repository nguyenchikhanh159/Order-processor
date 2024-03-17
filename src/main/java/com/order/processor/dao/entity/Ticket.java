package com.order.processor.dao.entity;

import lombok.*;

import javax.persistence.*;
import java.math.BigDecimal;
import java.util.Set;

@Builder
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@Entity
@Table(name = "ticket")
public class Ticket {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer id;

//    @ManyToOne
//    @JoinColumn(name="concert_id")
//    private Concert concert;

//    @OneToMany(mappedBy="ticket")
//    private Set<OrderDetail> orderDetails;

    @Column(name = "concert_id")
    private Integer concertId;

    @Column(name = "price")
    private BigDecimal price;

    @Column(name = "availability")
    private Integer availability;
}
