import psycopg2
from flask import Flask, jsonify, request

app = Flask(__name__)

def ejecutar_sql(consulta: str):
    connection = psycopg2.connect(
        host="localhost",
        port="5432",
        dbname="alexsoft",
        user="postgres",
        password="profesor1234",
        options="-c search_path=public"
    )
    cursor = connection.cursor()
    cursor.execute(consulta)
    columnas = [desc[0] for desc in cursor.description]
    resultados = cursor.fetchall()
    empleados = [dict(zip(columnas, fila)) for fila in resultados]
    connection.close()

    return empleados

# INICIO DE SESIÓN
@app.route('/login', methods=['POST'])
def login():
    body_request = request.json
    user = body_request["user"]
    passwd = body_request["passwd"]

    query = f'''
    SELECT *
    FROM public."Gestor"
    WHERE usuario = '{user}' AND passwd = '{passwd}';
    '''
    logged = ejecutar_sql(query)
    if len(logged) == 0:
        return jsonify({"msg": "Error al iniciar sesión"}), 401

    empleado_id = logged[0].get("empleado")
    empleado = ejecutar_sql(
        f"SELECT * FROM public.\"Empleado\" WHERE id = '{empleado_id}';"
    )
    if len(empleado) == 0:
        return jsonify({"msg": "Empleado no encontrado"}), 404

    return jsonify(
        {
            "id_empleado": empleado[0]["id"],
            "id_gestor": logged[0]["id"],
            "nombre": empleado[0]["nombre"],
            "email": empleado[0]["email"],
        }
    )


# PROYECTOS ACTIVOS
@app.route('/proyecto/proyectos_activos', methods=['GET'])
def obtener_proyectos_activos():
    try:
        query = '''
        SELECT * FROM public."Proyecto"
        WHERE (fecha_finalizacion IS NULL) OR (fecha_finalizacion > CURRENT_TIMESTAMP)
        ORDER BY id ASC 
        '''
        proyectos_activos = ejecutar_sql(query)

        return jsonify(proyectos_activos)

    except psycopg2.Error as e:
        return jsonify({"error": "motivo del error: " + str(e)}), 500


# PROYECTOS ACABADOS
@app.route('/proyecto/proyectos_acabados', methods=['GET'])
def obtener_proyectos_acabados():
    try:
        query = '''
        SELECT * FROM public."Proyecto"
        WHERE fecha_finalizacion < CURRENT_TIMESTAMP;
        '''
        proyectos = ejecutar_sql(query)

        return jsonify(proyectos)

    except psycopg2.Error as e:
        return jsonify({"error": str(e)}), 500


# OBTENER PROYECTOS DE UN GESTOR
@app.route('/proyecto/proyectos_gestor', methods=['GET'])
def obtener_proyectos_gestor_id():
    try:
        id_gestor = request.args.get('id')
        query = f'''
        SELECT p.* FROM public."GestoresProyecto" gp
        JOIN public."Proyecto" p ON gp.proyecto = p.id
        WHERE gp.gestor = {id_gestor} AND p.fecha_finalizacion > NOW();
        '''
        proyectos_gestor = ejecutar_sql(query)

        return jsonify(proyectos_gestor)

    except psycopg2.Error as e:
        return jsonify({"error": "motivo del error: " + str(e)}), 500


# ASIGNAR GESTOR A PROYECTO
@app.route('/asignar_gestor', methods=['POST'])
def asignar_gestor_a_proyecto():
    try:
        body_request = request.json
        gestor_id = body_request['gestor']
        proyecto_id = body_request['proyecto']

        query_gestor = 'SELECT id FROM public."Gestor" WHERE id = {gestor_id};'
        gestor = ejecutar_sql(query_gestor)
        if not gestor:
            return jsonify({"error": "El gestor indicado no existe"}), 404

        query_proyecto = 'SELECT id FROM public."Proyecto" WHERE id = {proyecto_id};'
        proyecto = ejecutar_sql(query_proyecto)
        if not proyecto:
            return jsonify({"error": "El proyecto indicado no existe"}), 404

        query_asignar = f'''
        INSERT INTO public."GestoresProyecto" (gestor, proyecto, fecha_asignacion)
        VALUES ({gestor_id}, {proyecto_id}, CURRENT_TIMESTAMP);
        '''
        ejecutar_sql(query_asignar)

        return jsonify({"mensaje": f"Gestor {gestor_id} asignado al proyecto {proyecto_id}"}), 201

    except psycopg2.Error as e:
        return jsonify({"error": str(e)}), 500


# OBTENER TAREAS DE UN PROYECTO
@app.route('/tareas', methods=['GET'])
def obtener_tareas_de_un_proyecto():
    try:
        proyecto_id = request.args.get('id', type=int)

        tareas = ejecutar_sql(f'SELECT * FROM public."Tarea" WHERE proyecto = {proyecto_id}')

        return jsonify(tareas)

    except psycopg2.Error as e:
        return jsonify({"error": str(e)}), 500


# CREAR TAREAS PARA UN PROYECTO
@app.route('/crear_tareas', methods=['POST'])
def crear_tareas_a_proyecto():
   try:
       body_request = request.json
       nombre = body_request['nombre']
       descripcion = body_request['descripcion']
       estimacion = body_request['estimacion']
       fecha_finalizacion = body_request['fecha_finalizacion']
       programador_id = body_request['programador']
       proyecto_id = body_request['proyecto']

       query_proyecto = f'SELECT id FROM public."Proyecto" WHERE id = {proyecto_id};'
       proyecto = ejecutar_sql(query_proyecto)
       if not proyecto:
           return jsonify({"error": "El proyecto indicado no existe"}), 404

       query_programador = f'SELECT id FROM public."Programador" WHERE id = {programador_id};'
       programador = ejecutar_sql(query_programador)
       if not programador:
           return jsonify({"error": "El programador indicado no existe"}), 404


       query_crear = f'''
       INSERT INTO public."Tarea" (nombre, descripcion, estimacion, fecha_creacion, fecha_finalizacion, programador, proyecto)
       VALUES (
           '{nombre}', 
           '{descripcion}', 
            {estimacion}, 
            CURRENT_TIMESTAMP, 
            '{fecha_finalizacion}', 
            {programador_id}, 
            {proyecto_id}
       );'''

       ejecutar_sql(query_crear)
       return jsonify({"message": "Tarea creada exitosamente"}), 201

   except psycopg2.Error as e:
       return jsonify({"error": str(e)}), 500


@app.route('/asignar_programador', methods=['POST'])
def asignar_programador_a_proyecto():
    try:
        body_request = request.json
        programador_id = body_request['programador']
        proyecto_id = body_request['proyecto']

        query_programador = f'SELECT id FROM public."Programador" WHERE id = {programador_id};'
        programador = ejecutar_sql(query_programador)
        if not programador:
            return jsonify({"error": "El programador indicado no existe"}), 404

        query_proyecto = 'SELECT id FROM public."Proyecto" WHERE id = {proyecto_id};'
        proyecto = ejecutar_sql(query_proyecto)
        if not proyecto:
            return jsonify({"error": "El proyecto indicado no existe"}), 404

        query_asignar = f'''
        INSERT INTO public."ProgramadoresProyecto" (programador, proyecto, fecha_asignacion)
        VALUES ({programador_id}, {proyecto_id}, CURRENT_TIMESTAMP);
        '''
        ejecutar_sql(query_asignar)

        return jsonify({"mensaje": f"Gestor {programador_id} asignado al proyecto {proyecto_id}"}), 201

    except psycopg2.Error as e:
        return jsonify({"error": str(e)}), 500



@app.route('/empleado/programadores',methods=['GET'])
def obtener_programadores():
   try:
       programadores = ejecutar_sql(
           """
           SELECT * FROM public."Programador"
           ORDER BY id ASC
           """
       )
       return programadores

   except psycopg2.Error as e:
       return jsonify({"error": str(e)}), 500






if __name__ == '__main__':
    app.run(debug=True)
