from flask import Flask, render_template, redirect, url_for, send_from_directory
from sqlalchemy import create_engine, text
import os
import pandas as pd

app = Flask(__name__)

# Configuración de base de datos (usa la misma que Streamlit)
DATABASE_URL = os.environ.get("DATABASE_URL")

def get_engine():
    """Obtiene conexión a la base de datos"""
    if DATABASE_URL:
        url = DATABASE_URL.replace("postgres://", "postgresql://")
        return create_engine(url)
    return None

def get_temporadas():
    """Obtiene lista de temporadas"""
    engine = get_engine()
    if not engine:
        return []
    
    with engine.connect() as conn:
        df = pd.read_sql(text("""
            SELECT id, nombre
            FROM temporadas
            WHERE activo = TRUE
            ORDER BY nombre DESC
        """), conn)
        return df.to_dict('records')

def get_equipos():
    """Obtiene lista de equipos"""
    engine = get_engine()
    if not engine:
        return []
    
    with engine.connect() as conn:
        df = pd.read_sql(text("""
            SELECT e.id, e.nombre, c.nombre as categoria,
                   e.nombre || ' ' || c.nombre as nombre_completo
            FROM equipos e
            JOIN categorias c ON e.categoria_id = c.id
            WHERE e.activo = TRUE
            ORDER BY c.nombre, e.nombre
        """), conn)
        return df.to_dict('records')

def get_equipo_stats(equipo_id, temporada_id=None):
    """Obtiene estadísticas de un equipo"""
    engine = get_engine()
    if not engine:
        return None
    
    with engine.connect() as conn:
        params = {"equipo_id": equipo_id}
        temp_filter = ""
        if temporada_id:
            temp_filter = "AND temporada_id = :temporada_id"
            params["temporada_id"] = temporada_id
        
        stats = pd.read_sql(text(f"""
            SELECT 
                COUNT(*) as partidos,
                COUNT(*) FILTER (WHERE sets_local > sets_visitante) as victorias,
                COUNT(*) FILTER (WHERE sets_local < sets_visitante) as derrotas,
                COALESCE(SUM(sets_local), 0) as sets_favor,
                COALESCE(SUM(sets_visitante), 0) as sets_contra
            FROM partidos_new
            WHERE equipo_id = :equipo_id
            {temp_filter}
        """), conn, params=params)
        
        if stats.empty:
            return None
        
        result = stats.iloc[0].to_dict()
        
        # Calcular racha actual
        ultimos = pd.read_sql(text(f"""
            SELECT 
                CASE WHEN sets_local > sets_visitante THEN 'W' ELSE 'L' END as resultado
            FROM partidos_new
            WHERE equipo_id = :equipo_id
            {temp_filter}
            ORDER BY fecha DESC
            LIMIT 5
        """), conn, params=params)
        
        if not ultimos.empty:
            result['racha'] = ''.join(ultimos['resultado'].tolist())
        else:
            result['racha'] = ''
        
        return result

def get_jugadores_equipo(equipo_id):
    """Obtiene jugadores de un equipo"""
    engine = get_engine()
    if not engine:
        return []
    
    with engine.connect() as conn:
        df = pd.read_sql(text("""
            SELECT 
                CASE 
                    WHEN nombre IS NOT NULL AND nombre != '' 
                    THEN nombre || ' ' || apellido 
                    ELSE apellido 
                END AS nombre_completo,
                dorsal,
                posicion
            FROM jugadores
            WHERE equipo_id = :equipo_id AND activo = TRUE
            ORDER BY dorsal NULLS LAST, apellido
        """), conn, params={"equipo_id": equipo_id})
        return df.to_dict('records')

def get_top_anotadores(equipo_id, temporada_id=None, limit=5):
    """Obtiene top anotadores del equipo"""
    engine = get_engine()
    if not engine:
        return []
    
    with engine.connect() as conn:
        params = {"equipo_id": equipo_id, "limit": limit}
        temp_filter = ""
        if temporada_id:
            temp_filter = "AND p.temporada_id = :temporada_id"
            params["temporada_id"] = temporada_id
        
        df = pd.read_sql(text(f"""
            SELECT 
                CASE 
                    WHEN j.nombre IS NOT NULL AND j.nombre != '' 
                    THEN j.nombre || ' ' || j.apellido 
                    ELSE j.apellido 
                END AS jugador,
                COUNT(*) FILTER (WHERE a.marca = '#') as puntos
            FROM acciones_new a
            JOIN jugadores j ON a.jugador_id = j.id
            JOIN partidos_new p ON a.partido_id = p.id
            WHERE p.equipo_id = :equipo_id
            AND a.tipo_accion IN ('atacar', 'bloqueo', 'saque')
            {temp_filter}
            GROUP BY j.id, j.nombre, j.apellido
            HAVING COUNT(*) FILTER (WHERE a.marca = '#') > 0
            ORDER BY puntos DESC
            LIMIT :limit
        """), conn, params=params)
        return df.to_dict('records')

def get_partidos_equipo(equipo_id, temporada_id=None, limit=None):
    """Obtiene partidos de un equipo"""
    engine = get_engine()
    if not engine:
        return []
    
    with engine.connect() as conn:
        params = {"equipo_id": equipo_id}
        temp_filter = ""
        limit_clause = ""
        
        if temporada_id:
            temp_filter = "AND temporada_id = :temporada_id"
            params["temporada_id"] = temporada_id
        
        if limit:
            limit_clause = f"LIMIT {limit}"
        
        df = pd.read_sql(text(f"""
            SELECT 
                id,
                rival,
                sets_local,
                sets_visitante,
                TO_CHAR(fecha, 'DD/MM/YYYY') as fecha,
                fecha as fecha_raw,
                CASE 
                    WHEN sets_local > sets_visitante THEN 'victoria'
                    WHEN sets_local < sets_visitante THEN 'derrota'
                    ELSE 'empate'
                END as resultado
            FROM partidos_new
            WHERE equipo_id = :equipo_id
            {temp_filter}
            ORDER BY fecha DESC
            {limit_clause}
        """), conn, params=params)
        return df.to_dict('records')

def get_todos_resultados(temporada_id=None):
    """Obtiene todos los resultados de todos los equipos"""
    engine = get_engine()
    if not engine:
        return []
    
    with engine.connect() as conn:
        params = {}
        temp_filter = ""
        
        if temporada_id:
            temp_filter = "WHERE p.temporada_id = :temporada_id"
            params["temporada_id"] = temporada_id
        
        df = pd.read_sql(text(f"""
            SELECT 
                p.id,
                e.nombre || ' ' || c.nombre as equipo,
                p.rival,
                p.sets_local,
                p.sets_visitante,
                TO_CHAR(p.fecha, 'DD/MM/YYYY') as fecha,
                p.fecha as fecha_raw,
                CASE 
                    WHEN p.sets_local > p.sets_visitante THEN 'victoria'
                    WHEN p.sets_local < p.sets_visitante THEN 'derrota'
                    ELSE 'empate'
                END as resultado
            FROM partidos_new p
            JOIN equipos e ON p.equipo_id = e.id
            JOIN categorias c ON e.categoria_id = c.id
            {temp_filter}
            ORDER BY p.fecha DESC
            LIMIT 50
        """), conn, params=params)
        return df.to_dict('records')

def get_clasificacion(equipo_id, temporada_id=None):
    """Obtiene posición aproximada en clasificación basada en victorias"""
    # Por ahora solo devolvemos las stats del equipo
    return get_equipo_stats(equipo_id, temporada_id)


# ========== RUTAS ==========

@app.route('/')
def index():
    """Página principal"""
    equipos = get_equipos()
    temporadas = get_temporadas()
    ultimos_resultados = get_todos_resultados()[:10]  # Últimos 10 partidos
    return render_template('index.html', 
                          equipos=equipos, 
                          temporadas=temporadas,
                          ultimos_resultados=ultimos_resultados)

@app.route('/equip/<int:equipo_id>')
def equipo(equipo_id):
    """Página de un equipo"""
    equipos = get_equipos()
    temporadas = get_temporadas()
    
    # Buscar el equipo seleccionado
    equipo_info = next((e for e in equipos if e['id'] == equipo_id), None)
    if not equipo_info:
        return redirect(url_for('index'))
    
    # Usar primera temporada activa por defecto
    temporada_id = temporadas[0]['id'] if temporadas else None
    
    stats = get_equipo_stats(equipo_id, temporada_id)
    jugadores = get_jugadores_equipo(equipo_id)
    top_anotadores = get_top_anotadores(equipo_id, temporada_id)
    partidos = get_partidos_equipo(equipo_id, temporada_id)
    
    return render_template('equipo.html', 
                          equipo=equipo_info,
                          equipos=equipos,
                          temporadas=temporadas,
                          temporada_actual=temporada_id,
                          stats=stats,
                          jugadores=jugadores,
                          top_anotadores=top_anotadores,
                          partidos=partidos)

@app.route('/resultats')
def resultados():
    """Página de todos los resultados"""
    equipos = get_equipos()
    temporadas = get_temporadas()
    temporada_id = temporadas[0]['id'] if temporadas else None
    resultados = get_todos_resultados(temporada_id)
    
    return render_template('resultados.html',
                          equipos=equipos,
                          temporadas=temporadas,
                          resultados=resultados)

@app.route('/login')
def login():
    """Redirige a la app de Streamlit"""
    # Cambia esta URL por la de tu app Streamlit
    return redirect("https://voleibolstats.com")

@app.route('/ads.txt')
def ads_txt():
    """Servir ads.txt para Google AdSense"""
    return send_from_directory('static', 'ads.txt')


if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
