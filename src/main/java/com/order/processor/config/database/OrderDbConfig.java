package com.order.processor.config.database;

import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import javax.persistence.EntityManagerFactory;
import javax.sql.DataSource;

@Slf4j
@Configuration
@EnableTransactionManagement
@EnableJpaRepositories(basePackages = {"com.order.processor.dao.repository"},
        entityManagerFactoryRef = "ordersEntityManagerFactory",
        transactionManagerRef = "ordersTransactionManager")
public class OrderDbConfig {

    @Value("${max.pool:50}")
    private int maxPool;

    @Value("${min.pool:10}")
    private int minPool;

    @Value("${orders.persistence.unit.name:orders}")
    private String persistenceUnitName;

    @Bean("orderDatasource")
    @ConfigurationProperties("orders.datasource")
    public DataSource ordersDataSource() {
        HikariDataSource hikariDataSource = DataSourceBuilder.create().type(HikariDataSource.class).build();
        hikariDataSource.setMaximumPoolSize(maxPool);
        hikariDataSource.setMinimumIdle(minPool);
        return hikariDataSource;
    }

    @Bean("ordersEntityManagerFactory")
    public LocalContainerEntityManagerFactoryBean ordersEntityManagerFactory(@Qualifier("orderDatasource") DataSource ordersDataSource) {

        LocalContainerEntityManagerFactoryBean entityManagerFactoryBean = new LocalContainerEntityManagerFactoryBean();
        entityManagerFactoryBean.setPersistenceUnitName(persistenceUnitName);
        entityManagerFactoryBean.setDataSource(ordersDataSource);
        entityManagerFactoryBean.setPackagesToScan(
                "com.order.processor.dao.entity"
        );
        entityManagerFactoryBean.setJpaVendorAdapter(new HibernateJpaVendorAdapter());
        return entityManagerFactoryBean;
    }

    @Bean("ordersTransactionManager")
    public PlatformTransactionManager pastesTransactionManager(@Qualifier("ordersEntityManagerFactory") EntityManagerFactory entityManagerFactory) {
        return new JpaTransactionManager(entityManagerFactory);
    }
}
