
package cl.minipedidos.kafka.controller;

import cl.minipedidos.kafka.model.Pedido;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

@RestController
@RequestMapping("/api/pedidos")
public class PedidoController {

    private final List<Pedido> pedidos = new ArrayList<>();
    private final AtomicLong secuencia = new AtomicLong(1000);

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public synchronized Pedido crearPedido(@RequestBody Pedido pedido) {

        pedido.setId(secuencia.incrementAndGet());
        pedido.setEstado("PENDIENTE");

        pedidos.add(pedido);

        return pedido;
    }

    @GetMapping
    public synchronized List<Pedido> listarPedidos() {
        return new ArrayList<>(pedidos);
    }
}
