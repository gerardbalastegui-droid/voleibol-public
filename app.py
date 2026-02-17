from flask import Flask, render_template, redirect, url_for, send_from_directory, request, session, g
from flask_babel import Babel, gettext as _, lazy_gettext as _l
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool
import os
import pandas as pd

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "voleibol-stats-secret-key-2024")

# Configuración de idiomas
app.config['LANGUAGES'] = ['ca', 'es', 'en']
app.config['BABEL_DEFAULT_LOCALE'] = 'ca'
app.config['BABEL_TRANSLATION_DIRECTORIES'] = 'translations'

babel = Babel()

def get_locale():
    """Determina el idioma a usar"""
    # 1. Primero comprobar si hay idioma en la URL (?lang=es)
    lang = request.args.get('lang')
    if lang and lang in app.config['LANGUAGES']:
        session['lang'] = lang
        return lang
    
    # 2. Comprobar si hay idioma guardado en sesión
    if 'lang' in session and session['lang'] in app.config['LANGUAGES']:
        return session['lang']
    
    # 3. Detectar del navegador
    return request.accept_languages.best_match(app.config['LANGUAGES'])

babel.init_app(app, locale_selector=get_locale)

# Diccionario de traducciones para valores de base de datos
TRADUCCIONES_POSICIONES = {
    'ca': {
        'Colocador': 'Col·locador',
        'Receptor': 'Receptor',
        'Opuesto': 'Opost',
        'Central': 'Central',
        'Líbero': 'Líbero'
    },
    'es': {
        'Colocador': 'Colocador',
        'Receptor': 'Receptor',
        'Opuesto': 'Opuesto',
        'Central': 'Central',
        'Líbero': 'Líbero'
    },
    'en': {
        'Colocador': 'Setter',
        'Receptor': 'Outside Hitter',
        'Opuesto': 'Opposite',
        'Central': 'Middle Blocker',
        'Líbero': 'Libero'
    }
}

@app.template_filter('traducir_posicion')
def traducir_posicion(posicion):
    """Filtro para traducir posiciones de jugadores"""
    if not posicion:
        return '-'
    lang = get_locale()
    traducciones = TRADUCCIONES_POSICIONES.get(lang, TRADUCCIONES_POSICIONES['ca'])
    return traducciones.get(posicion, posicion)

@app.context_processor
def inject_locale():
    """Inyecta el idioma actual en todos los templates"""
    return {
        'current_lang': get_locale(),
        'languages': [
            {'code': 'ca', 'name': 'Català', 'flag': '🏴󠁧󠁢󠁣󠁴󠁿'},
            {'code': 'es', 'name': 'Español', 'flag': '🇪🇸'},
            {'code': 'en', 'name': 'English', 'flag': '🇬🇧'}
        ]
    }


# Configuración de base de datos (usa la misma que Streamlit)
DATABASE_URL = os.environ.get("DATABASE_URL")

# Variable global para reusar el engine
_engine = None

def get_engine():
    """Obtiene conexión a la base de datos con pool limitado"""
    global _engine
    
    if _engine is not None:
        return _engine
    
    if DATABASE_URL:
        url = DATABASE_URL.replace("postgres://", "postgresql://")
        _engine = create_engine(
            url,
            poolclass=QueuePool,
            pool_size=2,
            max_overflow=3,
            pool_pre_ping=True,
            pool_recycle=300
        )
        return _engine
    return None

def get_equipos():
    """Obtiene lista de equipos"""
    engine = get_engine()
    if not engine:
        return []
    
    with engine.connect() as conn:
        df = pd.read_sql(text("""
            SELECT id, nombre, equipo_letra,
                   CASE 
                       WHEN equipo_letra IS NOT NULL AND equipo_letra != ''
                       THEN nombre || ' ' || equipo_letra
                       ELSE nombre
                   END as nombre_completo
            FROM equipos
            ORDER BY nombre, equipo_letra
        """), conn)
        return df.to_dict('records')

def get_equipo_stats(equipo_id):
    """Obtiene estadísticas de un equipo"""
    engine = get_engine()
    if not engine:
        return None
    
    with engine.connect() as conn:
        stats = pd.read_sql(text("""
            SELECT 
                COUNT(*) as partidos,
                COUNT(*) FILTER (WHERE 
                    (local = true AND SPLIT_PART(resultado, '-', 1)::int > SPLIT_PART(resultado, '-', 2)::int)
                    OR 
                    (local = false AND SPLIT_PART(resultado, '-', 2)::int > SPLIT_PART(resultado, '-', 1)::int)
                ) as victorias,
                COUNT(*) FILTER (WHERE 
                    (local = true AND SPLIT_PART(resultado, '-', 1)::int < SPLIT_PART(resultado, '-', 2)::int)
                    OR 
                    (local = false AND SPLIT_PART(resultado, '-', 2)::int < SPLIT_PART(resultado, '-', 1)::int)
                ) as derrotas
            FROM partidos_new
            WHERE equipo_id = :equipo_id
        """), conn, params={"equipo_id": equipo_id})
        
        if stats.empty:
            return None
        
        result = stats.iloc[0].to_dict()
        
        # Calcular racha actual (últimos 5 partidos)
        ultimos = pd.read_sql(text("""
            SELECT resultado, local
            FROM partidos_new
            WHERE equipo_id = :equipo_id AND resultado IS NOT NULL
            ORDER BY fecha DESC, id DESC
            LIMIT 5
        """), conn, params={"equipo_id": equipo_id})
        
        if not ultimos.empty:
            racha = []
            for _, row in ultimos.iterrows():
                try:
                    partes = row['resultado'].split('-')
                    sets_local = int(partes[0])
                    sets_visitante = int(partes[1])
                    
                    if row['local']:
                        # Si somos locales, ganamos si el primer número es mayor
                        victoria = sets_local > sets_visitante
                    else:
                        # Si somos visitantes, ganamos si el segundo número es mayor
                        victoria = sets_visitante > sets_local
                    
                    racha.append('W' if victoria else 'L')
                except:
                    racha.append('?')
            result['racha'] = racha
        else:
            result['racha'] = []
        
        return result

def get_jugadores_equipo(equipo_id):
    """Obtiene jugadores de un equipo"""
    engine = get_engine()
    if not engine:
        return []
    
    with engine.connect() as conn:
        df = pd.read_sql(text("""
            SELECT apellido, posicion, dorsal
            FROM jugadores
            WHERE equipo_id = :equipo_id AND activo = true
            ORDER BY dorsal NULLS LAST, apellido
        """), conn, params={"equipo_id": equipo_id})
        return df.to_dict('records')

def get_top_anotadores(equipo_id, limit=5):
    """Obtiene top anotadores del equipo"""
    engine = get_engine()
    if not engine:
        return []
    
    with engine.connect() as conn:
        df = pd.read_sql(text("""
            SELECT 
                j.apellido as jugador,
                COUNT(*) FILTER (WHERE a.marca = '#') as puntos
            FROM acciones_new a
            JOIN jugadores j ON a.jugador_id = j.id
            JOIN partidos_new p ON a.partido_id = p.id
            WHERE p.equipo_id = :equipo_id
            AND a.tipo_accion IN ('atacar', 'bloqueo', 'saque')
            GROUP BY j.id, j.apellido
            HAVING COUNT(*) FILTER (WHERE a.marca = '#') > 0
            ORDER BY puntos DESC
            LIMIT :limit
        """), conn, params={"equipo_id": equipo_id, "limit": limit})
        return df.to_dict('records')

def get_partidos_equipo(equipo_id, limit=10):
    """Obtiene partidos de un equipo (últimos 10 por defecto)"""
    engine = get_engine()
    if not engine:
        return []
    
    with engine.connect() as conn:
        limit_clause = f"LIMIT {limit}" if limit else ""
        
        df = pd.read_sql(text(f"""
            SELECT 
                id,
                rival,
                local,
                resultado,
                TO_CHAR(fecha, 'DD/MM/YYYY') as fecha_display,
                fecha as fecha_orden,
                CASE 
                    WHEN (local = true AND SPLIT_PART(resultado, '-', 1)::int > SPLIT_PART(resultado, '-', 2)::int)
                         OR (local = false AND SPLIT_PART(resultado, '-', 2)::int > SPLIT_PART(resultado, '-', 1)::int)
                    THEN 'victoria'
                    ELSE 'derrota'
                END as resultado_tipo
            FROM partidos_new
            WHERE equipo_id = :equipo_id
            ORDER BY fecha_orden DESC, id DESC
            {limit_clause}
        """), conn, params={"equipo_id": equipo_id})
        
        # Renombrar para mantener compatibilidad con el template
        df = df.rename(columns={'fecha_display': 'fecha'})
        df = df.drop(columns=['fecha_orden'])
        
        return df.to_dict('records')

def get_todos_resultados():
    """Obtiene todos los resultados de todos los equipos"""
    engine = get_engine()
    if not engine:
        return []
    
    with engine.connect() as conn:
        df = pd.read_sql(text("""
            SELECT 
                p.id,
                CASE 
                    WHEN e.equipo_letra IS NOT NULL AND e.equipo_letra != ''
                    THEN e.nombre || ' ' || e.equipo_letra
                    ELSE e.nombre
                END as equipo,
                p.rival,
                p.resultado,
                p.local,
                TO_CHAR(p.fecha, 'DD/MM/YYYY') as fecha,
                CASE 
                    WHEN (local = true AND SPLIT_PART(resultado, '-', 1)::int > SPLIT_PART(resultado, '-', 2)::int)
                        OR (local = false AND SPLIT_PART(resultado, '-', 2)::int > SPLIT_PART(resultado, '-', 1)::int)
                    THEN 'victoria'
                    ELSE 'derrota'
                END as resultado_tipo
            FROM partidos_new p
            JOIN equipos e ON p.equipo_id = e.id
            WHERE p.resultado IS NOT NULL
            ORDER BY p.fecha DESC
            LIMIT 50
        """), conn)
        return df.to_dict('records')


# ========== RUTAS ==========

@app.route('/')
def index():
    """Página principal"""
    equipos = get_equipos()
    ultimos_resultados = get_todos_resultados()[:10]
    return render_template('index.html', 
                          equipos=equipos, 
                          ultimos_resultados=ultimos_resultados)

@app.route('/equip/<int:equipo_id>')
def equipo(equipo_id):
    """Página de un equipo"""
    equipos = get_equipos()
    
    # Buscar el equipo seleccionado
    equipo_info = next((e for e in equipos if e['id'] == equipo_id), None)
    if not equipo_info:
        return redirect(url_for('index'))
    
    stats = get_equipo_stats(equipo_id)
    jugadores = get_jugadores_equipo(equipo_id)
    top_anotadores = get_top_anotadores(equipo_id)
    partidos = get_partidos_equipo(equipo_id)
    
    return render_template('equipo.html', 
                          equipo=equipo_info,
                          equipos=equipos,
                          stats=stats,
                          jugadores=jugadores,
                          top_anotadores=top_anotadores,
                          partidos=partidos)

@app.route('/resultats')
def resultados():
    """Página de todos los resultados"""
    equipos = get_equipos()
    resultados = get_todos_resultados()
    
    return render_template('resultados.html',
                          equipos=equipos,
                          resultados=resultados)

@app.route('/login')
def login():
    """Redirige a la app de Streamlit"""
    return redirect("https://app.voleibolstats.com")

@app.route('/ads.txt')
def ads_txt():
    """Servir ads.txt para Google AdSense"""
    return send_from_directory('static', 'ads.txt')

@app.route('/quisom')
def quisom():
    """Página Qui Som"""
    return render_template('quisom.html')

@app.route('/contacte')
def contacte():
    """Página de contacto"""
    return render_template('contacte.html')

@app.route('/privacitat')
def privacitat():
    """Política de privacitat"""
    return render_template('privacitat.html')

@app.route('/avis-legal')
def avis_legal():
    """Avís Legal"""
    return render_template('avis-legal.html')

@app.route('/cookies')
def cookies():
    """Política de Cookies"""
    return render_template('cookies.html')

@app.route('/com-funciona')
def com_funciona():
    """Com Funciona"""
    return render_template('com-funciona.html')

@app.route('/set-language/<lang>')
def set_language(lang):
    """Cambiar idioma"""
    if lang in app.config['LANGUAGES']:
        session['lang'] = lang
    # Volver a la página anterior o al inicio
    return redirect(request.referrer or url_for('index'))


if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
