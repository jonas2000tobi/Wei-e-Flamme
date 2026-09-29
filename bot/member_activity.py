from __future__ import annotations
import os
from datetime import datetime, timezone
import discord
try:
    from bot.module_registry import is_module_enabled
except Exception:
    from module_registry import is_module_enabled
try:
    from bot import runtime_db
except Exception:
    import runtime_db
try:
    from bot.role_keys import normalize_role_key
except Exception:
    from role_keys import normalize_role_key
try:
    from bot.guild_config import role_ids as guild_role_ids
except Exception:
    try:
        from guild_config import role_ids as guild_role_ids
    except Exception:
        guild_role_ids = None


def _day() -> str:
    return datetime.now(timezone.utc).date().isoformat()

def _ensure_tables() -> None:
    if not getattr(runtime_db, '_INITIALIZED', False):
        runtime_db.init_runtime_db()
    backend = getattr(runtime_db, '_BACKEND', 'sqlite')
    if backend == 'postgres':
        conn = runtime_db._pg_connect()
        try:
            with conn.cursor() as cur:
                cur.execute('''CREATE TABLE IF NOT EXISTS member_activity_daily (
                    guild_id BIGINT NOT NULL, user_id BIGINT NOT NULL, activity_date DATE NOT NULL,
                    messages INTEGER NOT NULL DEFAULT 0, reactions_given INTEGER NOT NULL DEFAULT 0,
                    reactions_received INTEGER NOT NULL DEFAULT 0, event_responses INTEGER NOT NULL DEFAULT 0,
                    last_activity_at TIMESTAMPTZ, PRIMARY KEY(guild_id,user_id,activity_date))''')
                cur.execute('''CREATE TABLE IF NOT EXISTS event_rsvp_transitions (
                    id BIGSERIAL PRIMARY KEY, guild_id BIGINT NOT NULL, user_id BIGINT NOT NULL, event_id TEXT NOT NULL,
                    old_choice TEXT NOT NULL DEFAULT '', new_choice TEXT NOT NULL DEFAULT '', changed_at TIMESTAMPTZ NOT NULL)''')
                cur.execute('''CREATE TABLE IF NOT EXISTS member_membership (
                    guild_id BIGINT NOT NULL, user_id BIGINT NOT NULL, member_since TIMESTAMPTZ,
                    source TEXT NOT NULL DEFAULT 'discord_join', updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY(guild_id,user_id))''')
                cur.execute("UPDATE event_rsvp_transitions SET old_choice='SUPPORT' WHERE old_choice IN ('HEAL','HEALER')")
                cur.execute("UPDATE event_rsvp_transitions SET new_choice='SUPPORT' WHERE new_choice IN ('HEAL','HEALER')")
            conn.commit()
        finally: conn.close()
    else:
        conn = runtime_db._sqlite_connect()
        try:
            conn.executescript('''CREATE TABLE IF NOT EXISTS member_activity_daily (
                guild_id INTEGER NOT NULL, user_id INTEGER NOT NULL, activity_date TEXT NOT NULL,
                messages INTEGER NOT NULL DEFAULT 0, reactions_given INTEGER NOT NULL DEFAULT 0,
                reactions_received INTEGER NOT NULL DEFAULT 0, event_responses INTEGER NOT NULL DEFAULT 0,
                last_activity_at TEXT, PRIMARY KEY(guild_id,user_id,activity_date));
                CREATE TABLE IF NOT EXISTS event_rsvp_transitions (
                id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id INTEGER NOT NULL, user_id INTEGER NOT NULL, event_id TEXT NOT NULL,
                old_choice TEXT NOT NULL DEFAULT '', new_choice TEXT NOT NULL DEFAULT '', changed_at TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS member_membership (
                guild_id INTEGER NOT NULL, user_id INTEGER NOT NULL, member_since TEXT,
                source TEXT NOT NULL DEFAULT 'discord_join', updated_at TEXT NOT NULL,
                PRIMARY KEY(guild_id,user_id));''')
            conn.execute("UPDATE event_rsvp_transitions SET old_choice='SUPPORT' WHERE old_choice IN ('HEAL','HEALER')")
            conn.execute("UPDATE event_rsvp_transitions SET new_choice='SUPPORT' WHERE new_choice IN ('HEAL','HEALER')")
            conn.commit()
        finally: conn.close()

def _inc(guild_id:int,user_id:int,column:str,amount:int=1) -> None:
    if column not in {'messages','reactions_given','reactions_received','event_responses'}: return
    _ensure_tables(); now=datetime.now(timezone.utc).isoformat(); day=_day()
    backend=getattr(runtime_db,'_BACKEND','sqlite')
    if backend=='postgres':
        conn=runtime_db._pg_connect()
        try:
            with conn.cursor() as cur:
                cur.execute(f'''INSERT INTO member_activity_daily(guild_id,user_id,activity_date,{column},last_activity_at)
                    VALUES(%s,%s,%s,%s,%s) ON CONFLICT(guild_id,user_id,activity_date)
                    DO UPDATE SET {column}=member_activity_daily.{column}+EXCLUDED.{column}, last_activity_at=EXCLUDED.last_activity_at''',
                    (int(guild_id),int(user_id),day,int(amount),now))
            conn.commit()
        finally: conn.close()
    else:
        conn=runtime_db._sqlite_connect()
        try:
            conn.execute(f'''INSERT INTO member_activity_daily(guild_id,user_id,activity_date,{column},last_activity_at)
                VALUES(?,?,?,?,?) ON CONFLICT(guild_id,user_id,activity_date)
                DO UPDATE SET {column}=member_activity_daily.{column}+excluded.{column}, last_activity_at=excluded.last_activity_at''',
                (int(guild_id),int(user_id),day,int(amount),now)); conn.commit()
        finally: conn.close()

def _configured_member_role_ids(guild_id:int) -> set[int]:
    if not callable(guild_role_ids): return set()
    out:set[int]=set()
    for kind in ("member","leader","advisor","guardian"):
        try: out.update(int(x) for x in (guild_role_ids(int(guild_id),kind) or []) if int(x))
        except Exception: pass
    return out

def _is_guild_member(member:discord.Member) -> bool:
    configured=_configured_member_role_ids(member.guild.id)
    if not configured: return True
    return bool(configured.intersection({int(r.id) for r in getattr(member,'roles',[]) if getattr(r,'id',None)}))

def _touch_membership(member:discord.Member, *, force_role_start:bool=False) -> None:
    _ensure_tables(); now=datetime.now(timezone.utc).isoformat()
    joined=(member.joined_at or datetime.now(timezone.utc)).astimezone(timezone.utc).isoformat()
    since=now if force_role_start else joined
    source='member_role' if force_role_start else 'discord_join'
    backend=getattr(runtime_db,'_BACKEND','sqlite')
    if backend=='postgres':
        conn=runtime_db._pg_connect()
        try:
            with conn.cursor() as cur:
                if force_role_start:
                    cur.execute("""INSERT INTO member_membership(guild_id,user_id,member_since,source,updated_at)
                     VALUES(%s,%s,%s,%s,%s) ON CONFLICT(guild_id,user_id) DO UPDATE SET
                     member_since=CASE WHEN member_membership.source='discord_join' THEN EXCLUDED.member_since ELSE member_membership.member_since END,
                     source=CASE WHEN member_membership.source='discord_join' THEN 'member_role' ELSE member_membership.source END,
                     updated_at=EXCLUDED.updated_at""",(member.guild.id,member.id,since,source,now))
                else:
                    cur.execute("""INSERT INTO member_membership(guild_id,user_id,member_since,source,updated_at)
                     VALUES(%s,%s,%s,%s,%s) ON CONFLICT(guild_id,user_id) DO NOTHING""",(member.guild.id,member.id,since,source,now))
            conn.commit()
        finally: conn.close()
    else:
        conn=runtime_db._sqlite_connect()
        try:
            if force_role_start:
                row=conn.execute('SELECT source FROM member_membership WHERE guild_id=? AND user_id=?',(member.guild.id,member.id)).fetchone()
                if row and str(row['source'] if hasattr(row,'keys') else row[0])=='discord_join':
                    conn.execute('UPDATE member_membership SET member_since=?,source=?,updated_at=? WHERE guild_id=? AND user_id=?',(since,'member_role',now,member.guild.id,member.id))
                elif not row:
                    conn.execute('INSERT INTO member_membership(guild_id,user_id,member_since,source,updated_at) VALUES(?,?,?,?,?)',(member.guild.id,member.id,since,'member_role',now))
            else:
                conn.execute('INSERT OR IGNORE INTO member_membership(guild_id,user_id,member_since,source,updated_at) VALUES(?,?,?,?,?)',(member.guild.id,member.id,since,source,now))
            conn.commit()
        finally: conn.close()

async def setup_member_activity(client:discord.Client, tree) -> None:
    _ensure_tables()
    async def on_message(message:discord.Message):
        if not message.guild or message.author.bot or not is_module_enabled(message.guild.id,'member_activity'): return
        if isinstance(message.author,discord.Member) and not _is_guild_member(message.author): return
        _touch_membership(message.author); _inc(message.guild.id,message.author.id,'messages')
    async def on_raw_reaction_add(payload:discord.RawReactionActionEvent):
        if not payload.guild_id or not is_module_enabled(payload.guild_id,'member_activity'): return
        guild=client.get_guild(payload.guild_id)
        if guild and client.user and payload.user_id==client.user.id: return
        reactor=guild.get_member(int(payload.user_id)) if guild else None
        if reactor is not None and not _is_guild_member(reactor): return
        _inc(payload.guild_id,payload.user_id,'reactions_given')
        author_id=int(getattr(payload,'message_author_id',0) or 0)
        if author_id and author_id != payload.user_id:
            author=guild.get_member(author_id) if guild else None
            if author is None or _is_guild_member(author): _inc(payload.guild_id,author_id,'reactions_received')
    async def on_member_join(member:discord.Member):
        if not member.bot: _touch_membership(member)
    async def on_member_update(before:discord.Member, after:discord.Member):
        if after.bot: return
        configured=_configured_member_role_ids(after.guild.id)
        if not configured: return
        before_ids={int(r.id) for r in getattr(before,'roles',[]) if getattr(r,'id',None)}
        after_ids={int(r.id) for r in getattr(after,'roles',[]) if getattr(r,'id',None)}
        if not before_ids.intersection(configured) and after_ids.intersection(configured):
            _touch_membership(after, force_role_start=True)
    client.add_listener(on_message,'on_message')
    client.add_listener(on_raw_reaction_add,'on_raw_reaction_add')
    client.add_listener(on_member_join,'on_member_join')
    client.add_listener(on_member_update,'on_member_update')
    for guild in client.guilds:
        configured=_configured_member_role_ids(guild.id)
        for member in guild.members:
            if member.bot: continue
            if not configured or _is_guild_member(member): _touch_membership(member)


def record_event_response(guild_id:int,user_id:int,event_id:str,old_choice:str,new_choice:str)->None:
    _inc(guild_id,user_id,'event_responses')
    old_choice=normalize_role_key(old_choice); new_choice=normalize_role_key(new_choice)
    if not old_choice or old_choice==new_choice: return
    _ensure_tables(); now=datetime.now(timezone.utc).isoformat(); backend=getattr(runtime_db,'_BACKEND','sqlite')
    vals=(int(guild_id),int(user_id),str(event_id),old_choice,new_choice,now)
    if backend=='postgres':
        conn=runtime_db._pg_connect()
        try:
            with conn.cursor() as cur: cur.execute('INSERT INTO event_rsvp_transitions(guild_id,user_id,event_id,old_choice,new_choice,changed_at) VALUES(%s,%s,%s,%s,%s,%s)',vals)
            conn.commit()
        finally: conn.close()
    else:
        conn=runtime_db._sqlite_connect()
        try: conn.execute('INSERT INTO event_rsvp_transitions(guild_id,user_id,event_id,old_choice,new_choice,changed_at) VALUES(?,?,?,?,?,?)',vals); conn.commit()
        finally: conn.close()
