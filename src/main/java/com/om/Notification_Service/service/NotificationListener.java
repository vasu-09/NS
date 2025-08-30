package com.om.Notification_Service.service;

import com.om.Notification_Service.config.RabbitConfig;
import com.om.Notification_Service.dto.EventMessage;
import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

@Service
public class NotificationListener {

    private final NotificationService notificationService;

    private static final Logger log = LoggerFactory.getLogger(NotificationListener.class);

    public NotificationListener(NotificationService ns) {
        this.notificationService = ns;
    }

    @RabbitListener(queues = RabbitConfig.QUEUE, ackMode = "MANUAL")
    public void onEvent(EventMessage event,
                        Channel channel,
                        @Header(AmqpHeaders.DELIVERY_TAG) long tag) {
        try {
            if (isValid(event)) {
                notificationService.handleEvent(event);
                channel.basicAck(tag, false);
            } else {
                log.warn("Discarding invalid event: {}", event);
                channel.basicAck(tag, false);
            }
        } catch (Exception e) {
            log.error("Failed to process event {}", event, e);
            try {
                channel.basicNack(tag, false, false); // route to DLQ
            } catch (Exception nackEx) {
                log.error("Failed to NACK message", nackEx);
            }
        }
    }

    private boolean isValid(EventMessage e) {
        boolean hasRecipients = e.getRecipientIds() != null && !e.getRecipientIds().isEmpty();
        if (!hasRecipients && e.getUserId() == null) return false;
        if (e.getType() == null) return false;
        // Add per-type validation, e.g., MEETING_REMINDER needs startTime, roomId, etc.
        return true;
    }
}

