
package cl.minipedidos.kafka.producer;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class PedidoProducer {

    private static final String TOPICO = "pedidos-creados";

    private final KafkaTemplate<String, String> kafkaTemplate;

    public PedidoProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void enviarPedido(String pedido) {
        kafkaTemplate.send(TOPICO, pedido);
        System.out.println("Pedido enviado a Kafka: " + pedido);
    }
}
