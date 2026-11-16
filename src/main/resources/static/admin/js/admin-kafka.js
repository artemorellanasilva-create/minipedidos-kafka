
/*
 * MiniPedidos V2 - Sesion 3 - Parte 2
 * Panel de administracion de Apache Kafka
 *
 * Operaciones:
 * 1. Listar topicos
 * 2. Consultar particiones y replicas
 * 3. Consultar configuraciones
 * 4. Crear topicos
 *
 * Pendiente:
 * 5. Modificar retencion
 */

"use strict";

document.addEventListener("DOMContentLoaded", () => {

    // ==========================================
    // 1. ELEMENTOS DEL FORMULARIO
    // ==========================================

    const formulario =
        document.getElementById("form-administracion");

    const operacion =
        document.getElementById("operacion");

    const grupoTopico =
        document.getElementById("grupo-topico");

    const grupoParticiones =
        document.getElementById("grupo-particiones");

    const grupoReplicas =
        document.getElementById("grupo-replicas");

    const grupoRetencion =
        document.getElementById("grupo-retencion");

    const grupoAutorizacion =
        document.getElementById("grupo-autorizacion");

    const nombreTopico =
        document.getElementById("topico");

    const particiones =
        document.getElementById("particiones");

    const replicas =
        document.getElementById("replicas");

    const retencion =
        document.getElementById("retencion");

    const tokenAdmin =
        document.getElementById("token-admin");

    const boton =
        document.getElementById("btn-ejecutar");

    const estado =
        document.getElementById("estado-operacion");

    const cuerpoTabla =
        document.getElementById("cuerpo-resultados");

    const detalle =
        document.getElementById("detalle-resultados");

    const API = "/api/admin/kafka";

    const operacionesDisponibles = [
        "listar",
        "consultar",
        "configuracion",
        "crear"
    ];

    // ==========================================
    // 2. ACTUALIZAR EL FORMULARIO
    // ==========================================

    function actualizarFormulario() {

        const seleccion = operacion.value;

        const esCreacion = seleccion === "crear";
        const esRetencion = seleccion === "retencion";

        grupoTopico.hidden = seleccion === "listar";

        grupoParticiones.hidden = !esCreacion;
        grupoReplicas.hidden = !esCreacion;

        grupoRetencion.hidden = !esRetencion;

        grupoAutorizacion.hidden =
            !(esCreacion || esRetencion);

        nombreTopico.required = seleccion !== "listar";

        particiones.required = esCreacion;
        replicas.required = esCreacion;

        retencion.required = esRetencion;

        tokenAdmin.required = esCreacion || esRetencion;

        // No conservar el token al cambiar de operacion
        tokenAdmin.value = "";

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
    // 3. AGREGAR RESULTADOS A LA TABLA
    // ==========================================

    function agregarFila(
            nombre,
            cantidad,
            replicasTexto) {

        const fila = document.createElement("tr");

        for (const valor of [
            nombre,
            cantidad,
            replicasTexto
        ]) {

            const celda = document.createElement("td");

            celda.textContent = String(valor);

            fila.appendChild(celda);
        }

        cuerpoTabla.appendChild(fila);
    }

    // ==========================================
    // 4. PROCESAR RESPUESTAS HTTP
    // ==========================================

    async function procesarRespuesta(respuesta) {

        const datos = await respuesta.json();

        if (!respuesta.ok) {

            let mensaje =
                `Error HTTP ${respuesta.status}`;

            if (datos && datos.error) {

                mensaje = datos.error;

            } else if (respuesta.status === 404) {

                mensaje = "El tópico no existe.";

            } else if (respuesta.status === 403) {

                mensaje = "Acceso administrativo denegado.";

            } else if (respuesta.status === 409) {

                mensaje = "El tópico ya existe.";

            } else if (respuesta.status === 503) {

                mensaje = "Kafka no está disponible.";
            }

            throw new Error(mensaje);
        }

        return datos;
    }

    // ==========================================
    // 5. CONSULTAS GET
    // ==========================================

    async function consultarAPI(ruta) {

        const respuesta = await fetch(ruta, {

            method: "GET",

            headers: {
                "Accept": "application/json"
            },

            cache: "no-store"
        });

        return await procesarRespuesta(respuesta);
    }

    // ==========================================
    // 6. VALIDAR EL NOMBRE DEL TOPICO
    // ==========================================

    function obtenerNombreTopico() {

        const nombre = nombreTopico.value.trim();

        if (
            !/^[a-zA-Z0-9._-]{1,249}$/.test(nombre) ||
            nombre === "." ||
            nombre === ".."
        ) {

            throw new Error(
                "Nombre de tópico no válido."
            );
        }

        return nombre;
    }

    // ==========================================
    // 7. CREAR TOPICO CON POST
    // ==========================================

    async function crearTopico() {

        const nombre = obtenerNombreTopico();

        const cantidadParticiones =
            Number(particiones.value);

        const factorReplicacion =
            Number(replicas.value);

        if (
            !Number.isInteger(cantidadParticiones) ||
            cantidadParticiones < 1 ||
            cantidadParticiones > 20
        ) {

            throw new Error(
                "Las particiones deben estar entre 1 y 20."
            );
        }

        if (factorReplicacion !== 1) {

            throw new Error(
                "Este laboratorio utiliza replicación 1."
            );
        }

        const token = tokenAdmin.value;

        if (!token.trim()) {

            throw new Error(
                "Debe ingresar el token administrativo."
            );
        }

        const respuesta = await fetch(
            `${API}/topicos`,
            {

                method: "POST",

                headers: {
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                    "X-Admin-Token": token
                },

                body: JSON.stringify({
                    nombre: nombre,
                    particiones: cantidadParticiones,
                    replicas: factorReplicacion
                }),

                cache: "no-store"
            }
        );

        return await procesarRespuesta(respuesta);
    }

    // ==========================================
    // 8. EJECUTAR OPERACIONES
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

        estado.textContent =
            seleccion === "crear"
                ? "Creando tópico en Kafka..."
                : "Consultando Kafka...";

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

                for (const [clave, valor] of Object.entries(datos)) {

                    agregarFila(
                        clave,
                        valor,
                        "—"
                    );
                }

                detalle.textContent =
                    JSON.stringify(datos, null, 2);

                estado.textContent =
                    `Configuración consultada correctamente: ${nombre}`;

            // ----------------------------------
            // OPERACION 4: CREAR TOPICO
            // ----------------------------------

            } else if (seleccion === "crear") {

                const datos = await crearTopico();

                agregarFila(
                    datos.nombre,
                    datos.particiones,
                    datos.replicas
                );

                detalle.textContent =
                    JSON.stringify(datos, null, 2);

                estado.textContent =
                    `Tópico ${datos.nombre} creado correctamente.`;
            }

            estado.className = "mensaje-exito";

        } catch (error) {

            estado.textContent = error.message;

            estado.className = "mensaje-error";

        } finally {

            // Borrar siempre el token del formulario
            tokenAdmin.value = "";

            boton.disabled =
                !operacionesDisponibles.includes(
                    operacion.value
                );
        }
    }

    // ==========================================
    // 9. EVENTOS
    // ==========================================

    operacion.addEventListener(
        "change",
        actualizarFormulario
    );

    formulario.addEventListener(
        "submit",
        ejecutarOperacion
    );

    // Inicializar
    actualizarFormulario();

    console.log(
        "MiniPedidos V2: panel administrativo inicializado."
    );

});
