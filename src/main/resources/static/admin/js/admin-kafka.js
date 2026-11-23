
/*
 * ==========================================
 * MINIPEDIDOS V2 - SESION 3 - PARTE 3
 * PASOS 5.17 Y 5.18
 * ADMINISTRACION KAFKA Y SEGURIDAD
 * ==========================================
 *
 * 1. Listar topicos
 * 2. Consultar particiones y replicas
 * 3. Consultar configuraciones
 * 4. Crear topicos
 * 5. Modificar retencion
 * 6. Mostrar usuario autenticado
 * 7. Cerrar sesion
 */

"use strict";

document.addEventListener("DOMContentLoaded", () => {

    // ==========================================
    // 1. ELEMENTOS DEL DASHBOARD
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
    // 2. ELEMENTOS DE SEGURIDAD
    // ==========================================

    const nombreUsuario =
        document.getElementById("nombre-usuario");

    const rolUsuario =
        document.getElementById("rol-usuario");

    const formularioLogout =
        document.getElementById("form-cerrar-sesion");

    const botonLogout =
        document.getElementById("btn-cerrar-sesion");

    const csrfLogout =
        document.getElementById("csrf-logout");

    // ==========================================
    // 3. ENDPOINTS
    // ==========================================

    const API = "/api/admin/kafka";

    const API_CSRF = "/admin/csrf";

    const API_USUARIO = "/admin/usuario";

    const operacionesDisponibles = [
        "listar",
        "consultar",
        "configuracion",
        "crear",
        "retencion"
    ];

    // ==========================================
    // 4. ACTUALIZAR FORMULARIO
    // ==========================================

    function actualizarFormulario() {

        const seleccion = operacion.value;

        const esCreacion = seleccion === "crear";
        const esRetencion = seleccion === "retencion";

        grupoTopico.hidden =
            seleccion === "listar";

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
            ? "Esta operación no está disponible."
            : "Operación lista para ejecutar.";
    }

    // ==========================================
    // 5. AGREGAR FILAS A LA TABLA
    // ==========================================

    function agregarFila(nombre, cantidad, replicasTexto) {

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
    // 6. PROCESAR RESPUESTAS HTTP
    // ==========================================

    async function procesarRespuesta(respuesta) {

        const tipo =
            respuesta.headers.get("content-type") || "";

        let datos = null;

        if (tipo.includes("application/json")) {
            datos = await respuesta.json();
        }

        if (!respuesta.ok) {

            let mensaje = `Error HTTP ${respuesta.status}`;

            if (respuesta.status === 401) {
                mensaje = "La sesión no está autenticada.";
            } else if (respuesta.status === 403) {
                mensaje = "Acceso denegado. Verifica el rol y CSRF.";
            } else if (respuesta.status === 404) {
                mensaje = "El recurso solicitado no existe.";
            } else if (respuesta.status === 409) {
                mensaje = "El tópico ya existe.";
            } else if (respuesta.status === 503) {
                mensaje = "Kafka no está disponible.";
            }

            if (datos && typeof datos.error === "string") {
                mensaje = datos.error;
            }

            throw new Error(mensaje);
        }

        if (!tipo.includes("application/json")) {

            throw new Error(
                "El servidor no devolvió JSON. Comprueba la sesión."
            );
        }

        return datos;
    }

    // ==========================================
    // 7. CONSULTAS GET
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

        return procesarRespuesta(respuesta);
    }

    // ==========================================
    // 8. OBTENER CSRF
    // ==========================================

    async function obtenerCSRF() {

        const datos = await consultarAPI(API_CSRF);

        if (
            !datos ||
            typeof datos.token !== "string" ||
            !datos.token ||
            typeof datos.headerName !== "string" ||
            !datos.headerName ||
            typeof datos.parameterName !== "string" ||
            !datos.parameterName
        ) {

            throw new Error(
                "No fue posible obtener el token CSRF."
            );
        }

        return datos;
    }

    // ==========================================
    // 9. CONSULTAR USUARIO CONECTADO
    // ==========================================

    async function inicializarSesion() {

        if (
            !nombreUsuario ||
            !rolUsuario ||
            !formularioLogout ||
            !botonLogout ||
            !csrfLogout
        ) {
            console.error(
                "Faltan elementos de sesión en index.html."
            );
            return;
        }

        nombreUsuario.textContent = "Cargando...";
        rolUsuario.textContent = "Verificando permisos...";
        botonLogout.disabled = true;

        try {

            const usuario = await consultarAPI(API_USUARIO);

            const csrf = await obtenerCSRF();

            if (
                typeof usuario.usuario !== "string" ||
                !Array.isArray(usuario.roles)
            ) {
                throw new Error(
                    "Respuesta de usuario no válida."
                );
            }

            nombreUsuario.textContent = usuario.usuario;

            rolUsuario.textContent =
                "Rol: " + usuario.roles.join(", ");

            csrfLogout.name = csrf.parameterName;
            csrfLogout.value = csrf.token;

            botonLogout.disabled = false;

        } catch (error) {

            nombreUsuario.textContent =
                "Sesión no verificada";

            rolUsuario.textContent = error.message;

            botonLogout.disabled = true;

            console.error(
                "Error al inicializar sesión:",
                error.message
            );
        }
    }

    // ==========================================
    // 10. VALIDAR NOMBRE DEL TOPICO
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
    // 11. OPERACIONES PROTEGIDAS
    // ==========================================

    async function enviarOperacion(ruta, metodo, datos) {

        const csrf = await obtenerCSRF();

        const respuesta = await fetch(ruta, {

            method: metodo,
            credentials: "same-origin",

            headers: {
                "Content-Type": "application/json",
                "Accept": "application/json",
                [csrf.headerName]: csrf.token
            },

            body: JSON.stringify(datos),
            cache: "no-store"
        });

        return procesarRespuesta(respuesta);
    }

    // ==========================================
    // 12. CREAR TOPICO
    // ==========================================

    async function crearTopico() {

        const nombre = obtenerNombreTopico();

        const cantidad = Number(particiones.value);

        const factor = Number(replicas.value);

        if (
            !Number.isInteger(cantidad) ||
            cantidad < 1 ||
            cantidad > 20
        ) {
            throw new Error(
                "Las particiones deben estar entre 1 y 20."
            );
        }

        if (factor !== 1) {
            throw new Error(
                "El laboratorio utiliza replicación 1."
            );
        }

        return enviarOperacion(
            `${API}/topicos`,
            "POST",
            {
                nombre: nombre,
                particiones: cantidad,
                replicas: factor
            }
        );
    }

    // ==========================================
    // 13. MODIFICAR RETENCION
    // ==========================================

    async function modificarRetencion() {

        const nombre = obtenerNombreTopico();

        const dias = Number(retencion.value);

        if (
            !Number.isInteger(dias) ||
            dias < 1 ||
            dias > 30
        ) {

            throw new Error(
                "La retención debe estar entre 1 y 30 días."
            );
        }

        return enviarOperacion(
            `${API}/topicos/${encodeURIComponent(nombre)}/retencion`,
            "PUT",
            {
                dias: dias
            }
        );
    }

    // ==========================================
    // 14. EJECUTAR OPERACIONES
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
                ? "Creando tópico..."
                : seleccion === "retencion"
                    ? "Modificando retención..."
                    : "Consultando Kafka...";

        cuerpoTabla.replaceChildren();
        detalle.textContent = "";

        try {

            // 1. LISTAR TOPICOS

            if (seleccion === "listar") {

                const nombres = await consultarAPI(
                    `${API}/topicos`
                );

                for (const nombre of nombres) {
                    agregarFila(nombre, "—", "—");
                }

                estado.textContent =
                    `Consulta exitosa: ${nombres.length} tópicos encontrados.`;

            // 2. CONSULTAR PARTICIONES

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

            // 3. CONSULTAR CONFIGURACIONES

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
                    `Configuración consultada: ${nombre}`;

            // 4. CREAR TOPICO

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

            // 5. MODIFICAR RETENCION

            } else if (seleccion === "retencion") {

                const datos = await modificarRetencion();

                agregarFila(
                    datos.nombre,
                    `${datos.dias} día(s)`,
                    `${datos.retencionMs} ms`
                );

                detalle.textContent =
                    JSON.stringify(datos, null, 2);

                estado.textContent =
                    `Retención actualizada a ${datos.dias} día(s) para ${datos.nombre}.`;
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
    // 15. CIERRE DE SESION
    // ==========================================

    if (formularioLogout) {

        formularioLogout.addEventListener(
            "submit",
            (evento) => {

                if (!csrfLogout.value) {

                    evento.preventDefault();

                    console.error(
                        "No hay CSRF disponible para cerrar sesión."
                    );

                    return;
                }

                botonLogout.disabled = true;
            }
        );
    }

    // ==========================================
    // 16. EVENTOS E INICIALIZACION
    // ==========================================

    operacion.addEventListener(
        "change",
        actualizarFormulario
    );

    formulario.addEventListener(
        "submit",
        ejecutarOperacion
    );

    actualizarFormulario();

    inicializarSesion();

    console.log(
        "MiniPedidos V2: dashboard inicializado."
    );

});
