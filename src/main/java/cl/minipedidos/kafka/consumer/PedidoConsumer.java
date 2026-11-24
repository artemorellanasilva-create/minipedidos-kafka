
package cl.minipedidos.kafka.consumer;

// ==========================================
// MINIPEDIDOS V2 - SESION 4
// PASO 4.6 - DOS CONSUMIDORES CONCURRENTES
// ==========================================

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class PedidoConsumer {

    private static final Logger logger =
            LoggerFactory.getLogger(PedidoConsumer.class);

    // ==========================================
    // 1. RECIBIR MENSAJES DESDE KAFKA
    // ==========================================

    @KafkaListener(
            topics = "pedidos-creados",
            groupId = "minipedidos-consumidores",
            concurrency = "3"
    )
    public void recibirPedido(String mensaje) {

        // Registrar el mensaje recibido
        logger.info(
                "Pedido recibido desde Kafka: {}",
                mensaje
        );

        // Registrar el hilo que procesa el pedido
        logger.info(
                "Hilo consumidor: {}",
                Thread.currentThread().getName()
        );
    }
}
