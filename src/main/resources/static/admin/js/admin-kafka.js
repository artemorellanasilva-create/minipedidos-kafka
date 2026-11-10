
/*
 * MiniPedidos V2 - Sesion 3
 * Panel de administracion de Kafka
 * Integracion con API REST
 *
 * Operaciones:
 * 1. Listar topicos
 * 2. Consultar particiones y replicas
 * 3. Consultar configuraciones
 */

"use strict";

document.addEventListener("DOMContentLoaded", () => {

    // ==========================================
    // 1. ELEMENTOS DEL FORMULARIO
    // ==========================================

    const formulario = document.getElementById("form-administracion");
    const operacion = document.getElementById("operacion");

    const grupoTopico = document.getElementById("grupo-topico");
    const grupoParticiones = document.getElementById("grupo-particiones");
    const grupoReplicas = document.getElementById("grupo-replicas");
    const grupoRetencion = document.getElementById("grupo-retencion");

    const nombreTopico = document.getElementById("topico");
    const particiones = document.getElementById("particiones");
    const replicas = document.getElementById("replicas");
    const retencion = document.getElementById("retencion");

    const boton = document.getElementById("btn-ejecutar");
    const estado = document.getElementById("estado-operacion");
    const cuerpoTabla = document.getElementById("cuerpo-resultados");
    const detalle = document.getElementById("detalle-resultados");

    const API = "/api/admin/kafka";

    // Operaciones habilitadas actualmente
    const operacionesDisponibles = [
        "listar",
        "consultar",
        "configuracion"
    ];

    // ==========================================
    // 2. ACTUALIZAR CAMPOS DEL FORMULARIO
    // ==========================================

    function actualizarFormulario() {

        const seleccion = operacion.value;

        grupoTopico.hidden = seleccion === "listar";

        grupoParticiones.hidden = seleccion !== "crear";
        grupoReplicas.hidden = seleccion !== "crear";
        grupoRetencion.hidden = seleccion !== "retencion";

        nombreTopico.required = seleccion !== "listar";
        particiones.required = seleccion === "crear";
        replicas.required = seleccion === "crear";
        retencion.required = seleccion === "retencion";

        boton.disabled =
            !operacionesDisponibles.includes(seleccion);

        cuerpoTabla.replaceChildren();
        detalle.textContent = "";

        estado.className = "";

        estado.textContent = boton.disabled
            ? "Esta operación todavía no está disponible."
            : "Operación lista para ejecutar.";
    }

    // ==========================================
    // 3. AGREGAR FILAS A LA TABLA
    // ==========================================

    function agregarFila(nombre, cantidad, replicasTexto) {

        const fila = document.createElement("tr");

        for (const valor of [nombre, cantidad, replicasTexto]) {

            const celda = document.createElement("td");

            celda.textContent = String(valor);

            fila.appendChild(celda);
        }

        cuerpoTabla.appendChild(fila);
    }

    // ==========================================
    // 4. CONSULTAR LA API REST
    // ==========================================

    async function consultarAPI(ruta) {

        const respuesta = await fetch(ruta, {
            method: "GET",
            headers: {
                "Accept": "application/json"
            },
            cache: "no-store"
        });

        if (!respuesta.ok) {

            let mensaje = `Error HTTP ${respuesta.status} al consultar Kafka`;

            if (respuesta.status === 404) {
                mensaje = "El tópico solicitado no existe.";
            }

            if (respuesta.status === 503) {
                mensaje = "El servicio Kafka no está disponible.";
            }

            throw new Error(mensaje);
        }

        return await respuesta.json();
    }

    // ==========================================
    // 5. VALIDAR NOMBRE DEL TOPICO
    // ==========================================

    function obtenerNombreTopico() {

        const nombre = nombreTopico.value.trim();

        if (
            !/^[a-zA-Z0-9._-]{1,249}$/.test(nombre) ||
            nombre === "." ||
            nombre === ".."
        ) {
            throw new Error("Nombre de tópico no válido.");
        }

        return nombre;
    }

    // ==========================================
    // 6. EJECUTAR OPERACIONES
    // ==========================================

    async function ejecutarOperacion(evento) {

        evento.preventDefault();

        const seleccion = operacion.value;

        if (!operacionesDisponibles.includes(seleccion)) {
            return;
        }

        if (!formulario.reportValidity()) {
            return;
        }

        boton.disabled = true;

        estado.className = "";
        estado.textContent = "Consultando Kafka...";

        cuerpoTabla.replaceChildren();
        detalle.textContent = "";

        try {

            // ----------------------------------
            // OPERACION 1: LISTAR TOPICOS
            // ----------------------------------

            if (seleccion === "listar") {

                const nombres = await consultarAPI(
                    `${API}/topicos`
                );

                for (const nombre of nombres) {

                    agregarFila(
                        nombre,
                        "—",
                        "—"
                    );
                }

                estado.textContent =
                    `Consulta exitosa: ${nombres.length} tópicos encontrados.`;

            // ----------------------------------
            // OPERACION 2: CONSULTAR PARTICIONES
            // ----------------------------------

            } else if (seleccion === "consultar") {

                const nombre = obtenerNombreTopico();

                const datos = await consultarAPI(
                    `${API}/topicos/${encodeURIComponent(nombre)}`
                );

                for (const p of datos.particiones) {

                    agregarFila(
                        `${datos.nombre} / P${p.particion}`,
                        `Líder: ${p.lider}`,
                        p.replicas.join(", ")
                    );
                }

                detalle.textContent =
                    JSON.stringify(datos, null, 2);

                estado.textContent =
                    `Consulta exitosa: ${datos.cantidadParticiones} particiones.`;

            // ----------------------------------
            // OPERACION 3: CONSULTAR CONFIGURACION
            // ----------------------------------

            } else if (seleccion === "configuracion") {

                const nombre = obtenerNombreTopico();

                const datos = await consultarAPI(
                    `${API}/topicos/${encodeURIComponent(nombre)}/configuracion`
                );

                cuerpoTabla.replaceChildren();

                // Presentar propiedades en tabla
                for (const [clave, valor] of Object.entries(datos)) {

                    agregarFila(
                        clave,
                        valor,
                        "—"
                    );
                }

                // Mostrar tambien el JSON completo
                detalle.textContent =
                    JSON.stringify(datos, null, 2);

                estado.textContent =
                    `Configuración consultada correctamente: ${nombre}`;
            }

            estado.className = "mensaje-exito";

        } catch (error) {

            estado.textContent = error.message;
            estado.className = "mensaje-error";

        } finally {

            boton.disabled =
                !operacionesDisponibles.includes(operacion.value);
        }
    }

    // ==========================================
    // 7. EVENTOS DEL FORMULARIO
    // ==========================================

    operacion.addEventListener(
        "change",
        actualizarFormulario
    );

    formulario.addEventListener(
        "submit",
        ejecutarOperacion
    );

    // Inicializar el formulario
    actualizarFormulario();

    console.log(
        "MiniPedidos V2: panel administrativo conectado a Kafka."
    );

});
