
#!/usr/bin/env python3

# ==========================================
# MINIPEDIDOS V2 - SESION 4
# PASO 4.9 - PRUEBA INTEGRAL
# SPRING SECURITY + CSRF + API + KAFKA
# ==========================================

import json
import getpass
import urllib.request
import urllib.parse
import urllib.error
import http.cookiejar

from html.parser import HTMLParser


BASE = "http://localhost:8080"


# ==========================================
# 1. POLITICA DE COOKIES LOCAL
# ==========================================

class PoliticaLocal(http.cookiejar.DefaultCookiePolicy):

    def return_ok_secure(self, cookie, request):

        if request.full_url.startswith(BASE + "/"):
            return True

        return super().return_ok_secure(
            cookie,
            request
        )


# ==========================================
# 2. EXTRAER CSRF DEL LOGIN
# ==========================================

class LectorCSRF(HTMLParser):

    def __init__(self):
        super().__init__()
        self.token = None

    def handle_starttag(self, tag, attrs):

        if tag != "input":
            return

        datos = dict(attrs)

        if datos.get("name") == "_csrf":
            self.token = datos.get("value")


# ==========================================
# 3. PROGRAMA PRINCIPAL
# ==========================================

def main():

    print("\n===================================")
    print("MINIPEDIDOS V2 - PRUEBA INTEGRAL")
    print("SESION 4 - PRODUCTOR Y CONSUMIDOR")
    print("===================================")

    # --------------------------------------
    # 3.1 PREPARAR SESION HTTP
    # --------------------------------------

    cookies = http.cookiejar.CookieJar(
        policy=PoliticaLocal()
    )

    cliente = urllib.request.build_opener(
        urllib.request.HTTPCookieProcessor(cookies)
    )

    # --------------------------------------
    # 3.2 OBTENER FORMULARIO DE LOGIN
    # --------------------------------------

    print("\n1. Consultando Spring Security...")

    respuesta = cliente.open(
        BASE + "/login",
        timeout=10
    )

    pagina = respuesta.read().decode("utf-8")

    lector = LectorCSRF()
    lector.feed(pagina)

    if not lector.token:
        raise RuntimeError(
            "No se encontro el token CSRF del login."
        )

    print("Formulario de login disponible.")

    # --------------------------------------
    # 3.3 AUTENTICAR ADMINISTRADOR
    # --------------------------------------

    print("\n2. Iniciando sesion...")

    usuario = input(
        "Usuario [admin]: "
    ).strip() or "admin"

    clave = getpass.getpass(
        "Contrasena (no se muestra): "
    )

    datos_login = urllib.parse.urlencode({
        "username": usuario,
        "password": clave,
        "_csrf": lector.token
    }).encode("utf-8")

    del clave

    solicitud_login = urllib.request.Request(
        BASE + "/login",
        data=datos_login,
        method="POST"
    )

    cliente.open(
        solicitud_login,
        timeout=10
    ).read()

    # --------------------------------------
    # 3.4 VERIFICAR USUARIO AUTENTICADO
    # --------------------------------------

    print("\n3. Verificando autenticacion...")

    respuesta_usuario = cliente.open(
        BASE + "/admin/usuario",
        timeout=10
    )

    tipo = respuesta_usuario.headers.get(
        "Content-Type", ""
    )

    if "application/json" not in tipo:
        raise RuntimeError(
            "No se recibio la informacion del usuario."
        )

    datos_usuario = json.load(
        respuesta_usuario
    )

    if datos_usuario.get("usuario") != usuario:
        raise RuntimeError(
            "No fue posible confirmar el inicio de sesion."
        )

    print(
        "Usuario autenticado:",
        datos_usuario["usuario"]
    )

    print(
        "Roles:",
        datos_usuario.get("roles", [])
    )

    # --------------------------------------
    # 3.5 OBTENER CSRF DE SESION
    # --------------------------------------

    print("\n4. Obteniendo token CSRF...")

    respuesta_csrf = cliente.open(
        BASE + "/admin/csrf",
        timeout=10
    )

    csrf = json.load(respuesta_csrf)

    if not csrf.get("token") or not csrf.get("headerName"):
        raise RuntimeError(
            "Token CSRF de la sesion no disponible."
        )

    print("Proteccion CSRF disponible.")

    # --------------------------------------
    # 3.6 PREPARAR PEDIDO
    # --------------------------------------

    print("\n5. Preparando pedido...")

    pedido = {
        "producto": "Notebook",
        "cantidad": 1,
        "precio": 450000
    }

    print(
        json.dumps(
            pedido,
            indent=2,
            ensure_ascii=False
        )
    )

    # --------------------------------------
    # 3.7 ENVIAR PEDIDO A API REST
    # --------------------------------------

    print("\n6. Enviando pedido a la API...")

    solicitud_pedido = urllib.request.Request(
        BASE + "/api/pedidos",

        data=json.dumps(
            pedido
        ).encode("utf-8"),

        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            csrf["headerName"]: csrf["token"]
        },

        method="POST"
    )

    respuesta_pedido = cliente.open(
        solicitud_pedido,
        timeout=15
    )

    print(
        "Codigo HTTP:",
        respuesta_pedido.status
    )

    datos_pedido = json.load(
        respuesta_pedido
    )

    print("\nPedido creado:")

    print(
        json.dumps(
            datos_pedido,
            indent=2,
            ensure_ascii=False
        )
    )

    # --------------------------------------
    # 3.8 RESULTADO DE LA PRUEBA
    # --------------------------------------

    if respuesta_pedido.status == 201:

        print("\n===================================")
        print("PRUEBA API REST EXITOSA")
        print("===================================")

        print(
            "Pedido creado con ID:",
            datos_pedido.get("id")
        )

        print(
            "\nVerificar ahora la terminal de"
            " Spring Boot."
        )

        print(
            "Debe aparecer el evento publicado"
            " y recibido desde Kafka."
        )

    else:

        print(
            "\nLa API respondio, pero no con HTTP 201."
        )


# ==========================================
# 4. EJECUCION
# ==========================================

if __name__ == "__main__":

    try:

        main()

    except urllib.error.HTTPError as error:

        print(
            "\nERROR HTTP:",
            error.code
        )

        print(
            "Verifica la autenticacion y CSRF."
        )

    except urllib.error.URLError as error:

        print(
            "\nERROR DE CONEXION:",
            error.reason
        )

        print(
            "Verifica que Spring Boot este activo."
        )

    except Exception as error:

        print(
            "\nERROR:",
            error
        )
