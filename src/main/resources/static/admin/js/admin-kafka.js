
/*
 * ==========================================
 * MINIPEDIDOS V2 - SESION 3 - PARTE 3
 * DASHBOARD DE ADMINISTRACION KAFKA
 * INTEGRACION CON SPRING SECURITY Y CSRF
 * ==========================================
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

    const nombreTopico =
        document.getElementById("topico");

    const particiones =
        document.getElementById("particiones");

    const replicas =
        document.getElementById("replicas");

    const retencion =
        document.getElementById("retencion");

    const boton =
        document.getElementById("btn-ejecutar");

    const estado =
        document.getElementById("estado-operacion");

    const cuerpoTabla =
        document.getElementById("cuerpo-resultados");

    const detalle =
        document.getElementById("detalle-resultados");

    // ==========================================
    // 2. DIRECCIONES DE LA API
    // ==========================================

    const API = "/api/admin/kafka";

    /*
     * Este endpoint se implementara en el
     * siguiente paso, en Spring Boot.
     *
     * Entregara el token CSRF correspondiente
     * a la sesion autenticada.
     */
    const API_CSRF = "/admin/csrf";

    const operacionesDisponibles = [
        "listar",
        "consultar",
        "configuracion",
        "crear"
    ];

    // ==========================================
    // 3. ACTUALIZAR EL FORMULARIO
    // ==========================================

    function actualizarFormulario() {

        const seleccion = operacion.value;

        const esCreacion = seleccion === "crear";
        const esRetencion = seleccion === "retencion";

        grupoTopico.hidden = seleccion === "listar";

        grupoParticiones.hidden = !esCreacion;
        grupoReplicas.hidden = !esCreacion;

        grupoRetencion.hidden = !esRetencion;

        nombreTopico.required =
            seleccion !== "listar";

        particiones.required = esCreacion;
        replicas.required = esCreacion;

        retencion.required = esRetencion;

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
    // 4. AGREGAR RESULTADOS A LA TABLA
    // ==========================================

    function agregarFila(
            nombre,
            cantidad,
            replicasTexto) {

        const fila = document.createElement("tr");

        const valores = [
            nombre,
            cantidad,
            replicasTexto
        ];

        for (const valor of valores) {

            const celda = document.createElement("td");

            celda.textContent = String(valor);

            fila.appendChild(celda);
        }

        cuerpoTabla.appendChild(fila);
    }

    // ==========================================
    // 5. PROCESAR RESPUESTAS HTTP
    // ==========================================

    async function procesarRespuesta(respuesta) {

        const tipoContenido =
            respuesta.headers.get("content-type") || "";

        let datos = null;

        if (tipoContenido.includes("application/json")) {
            datos = await respuesta.json();
        }

        if (!respuesta.ok) {

            let mensaje =
                `Error HTTP ${respuesta.status}`;

            if (respuesta.status === 401) {

                mensaje =
                    "La sesión no está autenticada. Inicia sesión nuevamente.";

            } else if (respuesta.status === 403) {

                mensaje =
                    "Acceso denegado. Verifica tus permisos o la sesión CSRF.";

            } else if (respuesta.status === 404) {

                mensaje = "El recurso solicitado no existe.";

            } else if (respuesta.status === 409) {

                mensaje = "El tópico ya existe.";

            } else if (respuesta.status === 503) {

                mensaje = "El servicio Kafka no está disponible.";
            }

            if (datos && datos.error) {
                mensaje = datos.error;
            }

            throw new Error(mensaje);
        }

        if (!tipoContenido.includes("application/json")) {

            throw new Error(
                "El servidor no devolvió JSON. Verifica que tu sesión siga activa."
            );
        }

        return datos;
    }

    // ==========================================
    // 6. CONSULTAS GET
    // ==========================================

    async function consultarAPI(ruta) {

        const respuesta = await fetch(ruta, {

            method: "GET",

            credentials: "same-origin",

            headers: {
                "Accept": "application/json"
            },

            cache: "no-store"
        });

        return await procesarRespuesta(respuesta);
    }

    // ==========================================
    // 7. OBTENER TOKEN CSRF
    // ==========================================

    async function obtenerCSRF() {

        const datos = await consultarAPI(API_CSRF);

        if (
            !datos ||
            typeof datos.token !== "string" ||
            !datos.token ||
            typeof datos.headerName !== "string" ||
            !datos.headerName
        ) {

            throw new Error(
                "No fue posible obtener la protección CSRF de la sesión."
            );
        }

        return {
            token: datos.token,
            headerName: datos.headerName
        };
    }

    // ==========================================
    // 8. VALIDAR NOMBRE DEL TOPICO
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
    // 9. CREAR TOPICO CON SPRING SECURITY
    // ==========================================

    async function crearTopico() {

        const nombre = obtenerNombreTopico();

        const cantidadParticiones =
            Number(particiones.value);

        const factorReplicacion =
            Number(replicas.value);

        // Validar particiones

        if (
            !Number.isInteger(cantidadParticiones) ||
            cantidadParticiones < 1 ||
            cantidadParticiones > 20
        ) {

            throw new Error(
                "Las particiones deben estar entre 1 y 20."
            );
        }

        // Validar replicas

        if (factorReplicacion !== 1) {

            throw new Error(
                "Este laboratorio utiliza replicación 1."
            );
        }

        // Obtener CSRF de la sesion autenticada

        const csrf = await obtenerCSRF();

        // Enviar solicitud POST a Spring Boot

        const respuesta = await fetch(
            `${API}/topicos`,
            {

                method: "POST",

                credentials: "same-origin",

                headers: {
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                    [csrf.headerName]: csrf.token
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
    // 10. EJECUTAR OPERACIONES
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

            boton.disabled =
                !operacionesDisponibles.includes(
                    operacion.value
                );
        }
    }

    // ==========================================
    // 11. EVENTOS
    // ==========================================

    operacion.addEventListener(
        "change",
        actualizarFormulario
    );

    formulario.addEventListener(
        "submit",
        ejecutarOperacion
    );

    // Inicializar formulario

    actualizarFormulario();

    console.log(
        "MiniPedidos V2: dashboard conectado a Spring Security."
    );

});
