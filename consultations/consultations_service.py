import json
import os
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from datetime import time as clock_time
from pathlib import Path
from typing import Annotated, Literal
from uuid import uuid4
from zoneinfo import ZoneInfo

import jwt
import redis
from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from sqlalchemy import (
    JSON,
    Boolean,
    Date,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    create_engine,
    delete,
    event,
    select,
)
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column
from sqlalchemy.pool import StaticPool

MSK = ZoneInfo("Europe/Moscow")
ACTIVE = {"pending", "confirmed", "reschedule"}
STATUSES = {"completed", "reschedule", "pending", "confirmed", "cancelled", "rejected"}
ROLE_NAMES = {
    "student": "Студент",
    "psychologist": "Психолог",
    "deputy": "Заместитель директора",
    "social": "Социальный педагог",
    "admin": "Администратор",
}
SPECIALISTS = [
    {"id": "psychologist", "name": "Соколова Кристина Николаевна", "room": "Кабинет 215"},
    {"id": "deputy", "name": "Морозов Алексей Викторович", "room": "Кабинет 301"},
    {"id": "social", "name": "Орлова Елена Андреевна", "room": "Кабинет 108"},
]
DEFAULT_RULES = {
    "start": "10:00",
    "end": "16:00",
    "breakStart": "13:00",
    "breakEnd": "14:00",
    "weekdays": [1, 2, 3, 4, 5],
}


def now() -> datetime:
    return datetime.now(MSK)


def stamp() -> str:
    return now().isoformat()


def fail(message: str, status: int = 409):
    raise HTTPException(status_code=status, detail=message)


class Base(DeclarativeBase):
    pass


class Role(Base):
    __tablename__ = "roles"
    id: Mapped[str] = mapped_column(String(24), primary_key=True)
    name: Mapped[str] = mapped_column(String(80))


class User(Base):
    __tablename__ = "users"
    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    name: Mapped[str] = mapped_column(String(160))
    group: Mapped[str] = mapped_column(String(40), default="")
    role_id: Mapped[str] = mapped_column(ForeignKey("roles.id"))


class Specialist(Base):
    __tablename__ = "specialists"
    id: Mapped[str] = mapped_column(String(40), primary_key=True)
    user_id: Mapped[str | None] = mapped_column(ForeignKey("users.id"), unique=True)
    profile: Mapped[dict] = mapped_column(JSON)


class Schedule(Base):
    __tablename__ = "schedules"
    specialist_id: Mapped[str] = mapped_column(ForeignKey("specialists.id"), primary_key=True)
    rules: Mapped[dict] = mapped_column(JSON)


class TimeSlot(Base):
    __tablename__ = "time_slots"
    id: Mapped[str] = mapped_column(String(100), primary_key=True)
    specialist_id: Mapped[str] = mapped_column(ForeignKey("specialists.id"))
    day: Mapped[date] = mapped_column(Date)
    time: Mapped[str] = mapped_column(String(5))
    duration: Mapped[int] = mapped_column(Integer, default=45)
    hidden: Mapped[bool] = mapped_column(Boolean, default=False)
    __table_args__ = (
        UniqueConstraint("specialist_id", "day", "time"),
        Index("ix_slots_specialist_day", "specialist_id", "day"),
    )


class Appointment(Base):
    __tablename__ = "appointments"
    id: Mapped[str] = mapped_column(String(40), primary_key=True)
    student_id: Mapped[str] = mapped_column(ForeignKey("users.id"))
    specialist_id: Mapped[str] = mapped_column(ForeignKey("specialists.id"))
    slot_id: Mapped[str] = mapped_column(ForeignKey("time_slots.id"))
    requested_slot_id: Mapped[str | None] = mapped_column(ForeignKey("time_slots.id"))
    status: Mapped[str] = mapped_column(String(24), default="pending")
    previous_status: Mapped[str | None] = mapped_column(String(24))
    topic: Mapped[str] = mapped_column(String(140), default="")
    private_comment: Mapped[str] = mapped_column(Text, default="")
    internal_notes: Mapped[str] = mapped_column(Text, default="")
    created_at: Mapped[str] = mapped_column(String(40), default=stamp)
    __table_args__ = (
        Index("ix_appointments_student_status", "student_id", "status"),
        Index("ix_appointments_specialist_status", "specialist_id", "status"),
    )


class Reservation(Base):
    __tablename__ = "slot_reservations"
    slot_id: Mapped[str] = mapped_column(ForeignKey("time_slots.id"), primary_key=True)
    appointment_id: Mapped[str] = mapped_column(ForeignKey("appointments.id"), index=True)


class AppointmentHistory(Base):
    __tablename__ = "appointment_history"
    id: Mapped[str] = mapped_column(String(40), primary_key=True, default=lambda: str(uuid4()))
    appointment_id: Mapped[str] = mapped_column(ForeignKey("appointments.id"), index=True)
    actor_id: Mapped[str] = mapped_column(ForeignKey("users.id"))
    actor: Mapped[str] = mapped_column(String(80))
    action: Mapped[str] = mapped_column(String(180))
    status: Mapped[str] = mapped_column(String(24))
    at: Mapped[str] = mapped_column(String(40), default=stamp)
    internal: Mapped[bool] = mapped_column(Boolean, default=False)


class Notification(Base):
    __tablename__ = "notifications"
    id: Mapped[str] = mapped_column(String(40), primary_key=True, default=lambda: str(uuid4()))
    student_id: Mapped[str] = mapped_column(ForeignKey("users.id"), index=True)
    text: Mapped[str] = mapped_column(String(240))
    at: Mapped[str] = mapped_column(String(40), default=stamp)
    read: Mapped[bool] = mapped_column(Boolean, default=False)


class AuditLog(Base):
    __tablename__ = "admin_audit_log"
    id: Mapped[str] = mapped_column(String(40), primary_key=True, default=lambda: str(uuid4()))
    actor_id: Mapped[str] = mapped_column(ForeignKey("users.id"))
    action: Mapped[str] = mapped_column(String(100))
    target: Mapped[str] = mapped_column(String(200))
    at: Mapped[str] = mapped_column(String(40), default=stamp)


class WriteLock(Base):
    __tablename__ = "consultation_write_lock"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    revision: Mapped[int] = mapped_column(Integer, default=0)


class Input(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Booking(Input):
    specialist_id: str = Field(min_length=1, max_length=40)
    slot_id: str = Field(min_length=1, max_length=100)
    topic: str = Field(default="", max_length=140)
    private_comment: str = Field(default="", max_length=600)


class Move(Input):
    slot_id: str = Field(min_length=1, max_length=100)


class Decision(Input):
    action: Literal["approve", "reject", "complete"]


class Notes(Input):
    notes: str = Field(max_length=2000)


class SlotInput(Input):
    day: date
    time: str = Field(pattern=r"^([01]\d|2[0-3]):[0-5]\d$")


class Rules(Input):
    start: str = Field(pattern=r"^([01]\d|2[0-3]):[0-5]\d$")
    end: str = Field(pattern=r"^([01]\d|2[0-3]):[0-5]\d$")
    breakStart: str = Field(pattern=r"^([01]\d|2[0-3]):[0-5]\d$")
    breakEnd: str = Field(pattern=r"^([01]\d|2[0-3]):[0-5]\d$")
    weekdays: list[int] = Field(min_length=1, max_length=7)

    @field_validator("weekdays")
    @classmethod
    def check_weekdays(cls, values):
        if len(set(values)) != len(values) or any(value not in range(7) for value in values):
            raise ValueError("Рабочие дни: уникальные числа от 0 до 6")
        return values

    @model_validator(mode="after")
    def check_hours(self):
        if not self.start <= self.breakStart < self.breakEnd <= self.end or self.start == self.end:
            raise ValueError("Проверьте часы работы и перерыв")
        return self


class ScheduleInput(Input):
    specialist_id: str = Field(min_length=1, max_length=40)
    from_day: date
    until_day: date
    rules: Rules

    @model_validator(mode="after")
    def check_period(self):
        if self.from_day < now().date() or not 0 <= (self.until_day - self.from_day).days <= 30:
            raise ValueError("Нужен период от сегодняшнего дня, не длиннее 31 дня")
        return self


class RoleInput(Input):
    role: Literal["student", "psychologist", "deputy", "social", "admin"]


class Assignment(Input):
    user_id: str = Field(min_length=1, max_length=128)


@dataclass(frozen=True)
class Actor:
    id: str
    role: str
    specialist_id: str | None = None


def slot_start(slot: TimeSlot) -> datetime:
    return datetime.combine(slot.day, clock_time.fromisoformat(slot.time), MSK)


def slot_key(specialist_id: str, day: date, time_value: str) -> str:
    return f"{specialist_id}:{day.isoformat()}:{time_value}"


def make_engine(url: str):
    options = {"pool_pre_ping": True}
    if url.startswith("sqlite"):
        options["connect_args"] = {"check_same_thread": False, "timeout": 15}
        if url.endswith(":memory:"):
            options["poolclass"] = StaticPool
    engine = create_engine(url, **options)
    if engine.dialect.name == "sqlite":

        @event.listens_for(engine, "connect")
        def sqlite_settings(connection, _):
            connection.execute("PRAGMA foreign_keys=ON")
            connection.execute("PRAGMA busy_timeout=15000")

    return engine


@contextmanager
def transaction(engine):
    with Session(engine, expire_on_commit=False) as session:
        try:
            if engine.dialect.name == "sqlite":
                session.connection().exec_driver_sql("BEGIN IMMEDIATE")
            lock = session.scalar(select(WriteLock).where(WriteLock.id == 1).with_for_update())
            if lock is None:
                fail("База данных не инициализирована", 503)
            yield session
            lock.revision += 1
            session.commit()
        except IntegrityError:
            session.rollback()
            fail("Интервал уже занят или такая запись существует. Обновите расписание")
        except Exception:
            session.rollback()
            raise


def get_item(session: Session, model, identifier):
    item = session.get(model, identifier)
    if item is None:
        fail("Запись не найдена", 404)
    return item


def rows(session: Session, model, **filters):
    return list(session.scalars(select(model).filter_by(**filters)))


def fields(item, *names, **aliases):
    return {name: getattr(item, name) for name in names} | {
        name: getattr(item, attribute) for name, attribute in aliases.items()
    }


def specialist_access(session: Session, actor: Actor, specialist_id: str):
    person = get_item(session, Specialist, specialist_id)
    if person.user_id != actor.id or actor.role not in {"psychologist", "social", "deputy"}:
        fail("Доступ к этому специалисту запрещён", 403)
    return person


def appointment_access(session: Session, actor: Actor, identifier: str, staff_only=False):
    item = get_item(session, Appointment, identifier)
    if not staff_only and actor.role == "student" and item.student_id == actor.id:
        return item
    specialist_access(session, actor, item.specialist_id)
    return item


def available(session: Session, slot_id: str, specialist_id: str, exclude: str | None = None):
    slot = get_item(session, TimeSlot, slot_id)
    if slot.specialist_id != specialist_id or slot.hidden or slot_start(slot) <= now():
        fail("Выберите доступный будущий интервал специалиста")
    reservation = session.get(Reservation, slot.id)
    if reservation and reservation.appointment_id != exclude:
        fail("Это время уже занято")
    return slot


def check_student_time(session: Session, student_id: str, slot: TimeSlot, exclude=None):
    items = session.scalars(
        select(Appointment).where(Appointment.student_id == student_id, Appointment.status.in_(ACTIVE))
    ).all()
    for item in items:
        if item.id == exclude:
            continue
        for identifier in {item.slot_id, item.requested_slot_id} - {None}:
            other = get_item(session, TimeSlot, identifier)
            if slot_start(slot) < slot_start(other) + timedelta(minutes=other.duration) and slot_start(
                other
            ) < slot_start(slot) + timedelta(minutes=slot.duration):
                fail("У студента уже есть консультация в это время")


def reserve(session: Session, item: Appointment):
    session.execute(delete(Reservation).where(Reservation.appointment_id == item.id))
    session.flush()
    if item.status in ACTIVE:
        for identifier in {item.slot_id, item.requested_slot_id} - {None}:
            session.add(Reservation(slot_id=identifier, appointment_id=item.id))
    session.flush()


def record(session: Session, item: Appointment, actor: Actor, action: str, *, internal=False, notify=True):
    session.add(
        AppointmentHistory(
            appointment_id=item.id,
            actor_id=actor.id,
            actor=ROLE_NAMES[actor.role],
            action=action,
            status=item.status,
            internal=internal,
        )
    )
    if notify and not internal:
        session.add(Notification(student_id=item.student_id, text=action))
    session.flush()


def audit(session: Session, actor: Actor, action: str, target: str):
    session.add(AuditLog(actor_id=actor.id, action=action, target=target))


def appointment_json(session: Session, item: Appointment, actor: Actor):
    student = get_item(session, User, item.student_id)
    specialist = get_item(session, Specialist, item.specialist_id)
    owns_private = specialist.user_id == actor.id and actor.role != "student"
    own_request = actor.id == item.student_id and actor.role == "student"
    history = session.scalars(
        select(AppointmentHistory)
        .where(AppointmentHistory.appointment_id == item.id)
        .order_by(AppointmentHistory.at, AppointmentHistory.id)
    ).all()
    result = fields(
        item,
        "id",
        "status",
        studentId="student_id",
        specialistId="specialist_id",
        slotId="slot_id",
        requestedSlotId="requested_slot_id",
        createdAt="created_at",
    )
    result.update(studentName=student.name, group=student.group)
    result["history"] = [
        fields(entry, "id", "actor", "status", "at", text="action")
        for entry in history
        if not entry.internal or owns_private
    ]
    if owns_private or own_request:
        result.update(topic=item.topic, privateComment=item.private_comment)
    if owns_private:
        result["internalNotes"] = item.internal_notes
    return result


def slot_json(slot: TimeSlot, occupied: bool):
    result = fields(slot, "id", "time", "duration", "hidden", specialistId="specialist_id")
    result.update(
        day=slot.day.isoformat(),
        occupied=occupied,
        available=not occupied and not slot.hidden and slot_start(slot) > now(),
    )
    return result


def notification_rows(session: Session, user_id: str):
    query = select(Notification).where(Notification.student_id == user_id).order_by(Notification.at.desc())
    return [
        fields(item, "id", "text", "at", "read", studentId="student_id") for item in session.scalars(query)
    ]


def add_slot(session: Session, specialist_id: str, payload: SlotInput):
    get_item(session, Specialist, specialist_id)
    identifier = slot_key(specialist_id, payload.day, payload.time)
    candidate = TimeSlot(
        id=identifier,
        specialist_id=specialist_id,
        day=payload.day,
        time=payload.time,
        duration=45,
        hidden=False,
    )
    if (
        slot_start(candidate) <= now()
        or slot_start(candidate).date() != (slot_start(candidate) + timedelta(minutes=44)).date()
    ):
        fail("Интервал должен быть в будущем и завершаться в тот же день")
    others = rows(session, TimeSlot, specialist_id=specialist_id, day=payload.day, hidden=False)
    for other in others:
        if slot_start(candidate) < slot_start(other) + timedelta(minutes=other.duration) and slot_start(
            other
        ) < slot_start(candidate) + timedelta(minutes=45):
            fail("Интервал пересекается с существующим временем")
    existing = session.get(TimeSlot, identifier)
    if existing:
        existing.hidden = False
        candidate = existing
    else:
        session.add(candidate)
    session.flush()
    return candidate


def seed_database(engine):
    Base.metadata.create_all(engine)
    with Session(engine) as session, session.begin():
        if session.get(WriteLock, 1) is None:
            session.add(WriteLock(id=1, revision=0))
        for identifier, label in ROLE_NAMES.items():
            if session.get(Role, identifier) is None:
                session.add(Role(id=identifier, name=label))
        session.flush()
        for person in SPECIALISTS:
            if session.get(Specialist, person["id"]) is None:
                profile = {**person, "role": ROLE_NAMES[person["id"]], "duration": 45}
                session.add(Specialist(id=person["id"], user_id=None, profile=profile))
                session.flush()
                session.add(Schedule(specialist_id=person["id"], rules=DEFAULT_RULES))


def seed_local_data(engine):
    with transaction(engine) as session:
        accounts = [
            ("local-student", "Студент", "2ИП-18-25", "student"),
            ("local-admin", "Администратор", "", "admin"),
        ]
        accounts += [("local-" + person["id"], person["name"], "", person["id"]) for person in SPECIALISTS]
        for identifier, name, group, role in accounts:
            if session.get(User, identifier) is None:
                session.add(User(id=identifier, name=name, group=group, role_id=role))
        session.flush()
        for person in SPECIALISTS:
            specialist = get_item(session, Specialist, person["id"])
            if specialist.user_id is None:
                specialist.user_id = "local-" + person["id"]
        if session.scalar(select(TimeSlot.id).limit(1)) is None:
            for offset in range(1, 15):
                day = now().date() + timedelta(days=offset)
                if day.weekday() < 5:
                    for person in SPECIALISTS:
                        for value in ("10:00", "11:00", "12:00", "14:00", "15:00"):
                            add_slot(session, person["id"], SlotInput(day=day, time=value))


def create_app(database_url=None, demo_mode=None, jwt_key=None, redis_url=None) -> FastAPI:
    url = database_url or os.getenv(
        "DATABASE_URL", "sqlite:///" + str(Path(__file__).with_name("consultations.db"))
    )
    key = jwt_key or os.getenv("JWT_KEY")
    mode = os.getenv("DEMO_MODE")
    local = demo_mode is None and mode is None and not key and url.startswith("sqlite")
    demo = demo_mode if demo_mode is not None else (mode == "1" or local)
    algorithm = os.getenv("JWT_ALGORITHM", "HS256")
    issuer = os.getenv("JWT_ISSUER", "itteam")
    audience = os.getenv("JWT_AUDIENCE", "consultations")
    if not demo and (not key or url.startswith("sqlite")):
        raise RuntimeError(
            "Для рабочего режима нужны PostgreSQL (DATABASE_URL) и ключ JWT_KEY. Для локального демо задайте DEMO_MODE=1"
        )
    if algorithm not in {"HS256", "RS256"}:
        raise RuntimeError("Поддерживаются только HS256 и RS256")
    engine = make_engine(url)
    seed_database(engine)
    if local:
        seed_local_data(engine)
    app = FastAPI(
        title="Консультации IT.Team",
        version="0.2.0",
        description="Микросервис записи на консультации. Для локальной проверки выберите роль в X-Demo-Role; по умолчанию — студент. При подключении IT.Team используется Bearer JWT.",
    )
    app.state.engine = engine
    app.state.demo = demo
    app.state.local = local
    app.state.demo_roles = local
    app.state.demo_reset = False
    redis_address = redis_url if redis_url is not None else os.getenv("REDIS_URL")
    cache = (
        redis.Redis.from_url(redis_address, decode_responses=True, socket_timeout=2, socket_connect_timeout=2)
        if redis_address
        else None
    )
    limits = {}
    limits_lock = threading.Lock()

    def limited(user_id: str):
        window = int(time.time() // 60)
        if cache:
            try:
                count = cache.eval(
                    "local n=redis.call('INCR',KEYS[1]); if n==1 then redis.call('EXPIRE',KEYS[1],70) end; return n",
                    1,
                    f"consultations:rate:{user_id}:{window}",
                )
            except redis.RedisError:
                fail("Сервис ограничения запросов временно недоступен", 503)
        else:
            with limits_lock:
                for old in [item for item in limits if item[1] != window]:
                    del limits[old]
                limits[(user_id, window)] = limits.get((user_id, window), 0) + 1
                count = limits[(user_id, window)]
        if count > 180:
            raise HTTPException(
                429, "Слишком много запросов. Попробуйте через минуту", headers={"Retry-After": "60"}
            )

    def actor(
        authorization: Annotated[str | None, Header()] = None,
        x_demo_role: Annotated[
            Literal["student", "psychologist", "deputy", "social", "admin"] | None, Header()
        ] = None,
    ):
        if local and authorization is None:
            role = x_demo_role or "student"
            identifier = "local-" + role
            limited(identifier)
            with Session(engine) as session:
                if role in {"psychologist", "deputy", "social"}:
                    identifier = get_item(session, Specialist, role).user_id
                user = get_item(session, User, identifier)
                person = session.scalar(select(Specialist).where(Specialist.user_id == user.id))
                return Actor(user.id, user.role_id, person.id if person else None)
        if not authorization or not authorization.startswith("Bearer ") or not key:
            fail("Нужен токен авторизации IT.Team", 401)
        try:
            claims = jwt.decode(
                authorization[7:],
                key,
                algorithms=[algorithm],
                audience=audience,
                issuer=issuer,
                options={"require": ["sub", "exp", "iss", "aud"]},
            )
            user_id = claims["sub"]
            role_id = claims.get("role", "student")
            if not isinstance(user_id, str) or not 1 <= len(user_id) <= 128 or role_id not in ROLE_NAMES:
                raise ValueError("Некорректные данные пользователя")
        except (jwt.PyJWTError, ValueError, TypeError):
            fail("Токен недействителен или истёк", 401)
        with Session(engine) as session:
            user = session.get(User, user_id)
            if user is not None and user.role_id != role_id:
                fail("Роль пользователя изменилась. Обновите сессию", 403)
            new_user = user is None
        if new_user:
            with transaction(engine) as session:
                user = session.get(User, user_id)
                if user is None:
                    session.add(
                        User(
                            id=user_id,
                            name=str(claims.get("name", "Студент"))[:160],
                            group=str(claims.get("group", ""))[:40],
                            role_id=role_id,
                        )
                    )
                elif user.role_id != role_id:
                    fail("Роль пользователя изменилась. Обновите сессию", 403)
        limited(user_id)
        with Session(engine) as session:
            user = get_item(session, User, user_id)
            person = session.scalar(select(Specialist).where(Specialist.user_id == user.id))
            return Actor(user.id, user.role_id, person.id if person else None)

    app.state.actor_dependency = actor
    app.state.limit_requests = limited
    Auth = Annotated[Actor, Depends(actor)]

    @app.get("/health")
    def health():
        with Session(engine) as session:
            session.execute(select(WriteLock.id).limit(1))
        return {"status": "ok", "demo": demo, "database": engine.dialect.name}

    @app.get("/api/config")
    def config():
        return {"demo": app.state.demo_roles, "reset": app.state.demo_reset}

    @app.get("/api/v1/view")
    def interface_view(current: Auth):
        if current.role not in {"student", "admin"} and current.specialist_id is None:
            fail("Сотрудник не назначен специалистом", 403)
        with Session(engine) as session:
            user = get_item(session, User, current.id)
            query = select(Appointment)
            slots_query = select(TimeSlot)
            if current.role == "student":
                query = query.where(Appointment.student_id == current.id)
            elif current.role != "admin":
                query = query.where(Appointment.specialist_id == current.specialist_id)
                slots_query = slots_query.where(TimeSlot.specialist_id == current.specialist_id)
            occupied = set(session.scalars(select(Reservation.slot_id)))
            return {
                "student": fields(user, "id", "name", "group"),
                "role": current.role,
                "specialists": [person.profile for person in rows(session, Specialist)],
                "slots": [slot_json(slot, slot.id in occupied) for slot in session.scalars(slots_query)],
                "appointments": [
                    appointment_json(session, item, current)
                    for item in session.scalars(query.order_by(Appointment.created_at.desc()))
                ],
                "notifications": notification_rows(session, current.id),
                "rules": {rule.specialist_id: rule.rules for rule in rows(session, Schedule)},
            }

    @app.get("/api/v1/specialists")
    def specialists(current: Auth):
        with Session(engine) as session:
            return [person.profile for person in session.scalars(select(Specialist).order_by(Specialist.id))]

    @app.get("/api/v1/specialists/{identifier}")
    def specialist(identifier: str, current: Auth):
        with Session(engine) as session:
            return get_item(session, Specialist, identifier).profile

    @app.get("/api/v1/specialists/{identifier}/slots")
    def slots(identifier: str, current: Auth, day: date | None = None):
        with Session(engine) as session:
            get_item(session, Specialist, identifier)
            revision = get_item(session, WriteLock, 1).revision
            cache_key = f"consultations:slots:{revision}:{identifier}:{day}"
            if cache:
                try:
                    stored = cache.get(cache_key)
                    if stored:
                        return [
                            slot
                            for slot in json.loads(stored)
                            if datetime.fromisoformat(slot["day"] + "T" + slot["time"]).replace(tzinfo=MSK)
                            > now()
                        ]
                except redis.RedisError:
                    pass
            query = select(TimeSlot).where(TimeSlot.specialist_id == identifier, TimeSlot.hidden.is_(False))
            if day:
                query = query.where(TimeSlot.day == day)
            occupied = set(session.scalars(select(Reservation.slot_id)))
            result = [
                slot_json(slot, False)
                for slot in session.scalars(query.order_by(TimeSlot.day, TimeSlot.time))
                if slot.id not in occupied and slot_start(slot) > now()
            ]
            if cache:
                try:
                    cache.setex(cache_key, 15, json.dumps(result))
                except redis.RedisError:
                    pass
            return result

    @app.post("/api/v1/appointments", status_code=201)
    def book(payload: Booking, current: Auth):
        if current.role != "student":
            fail("Запись создаёт студент", 403)
        with transaction(engine) as session:
            slot = available(session, payload.slot_id, payload.specialist_id)
            check_student_time(session, current.id, slot)
            item = Appointment(
                id=str(uuid4()),
                student_id=current.id,
                specialist_id=payload.specialist_id,
                slot_id=slot.id,
                status="pending",
                topic=payload.topic.strip(),
                private_comment=payload.private_comment.strip() if payload.specialist_id != "deputy" else "",
                internal_notes="",
                created_at=stamp(),
            )
            session.add(item)
            session.flush()
            reserve(session, item)
            record(session, item, current, "Заявка отправлена специалисту")
            result = appointment_json(session, item, current)
        return result

    @app.get("/api/v1/appointments/my")
    def mine(current: Auth):
        with Session(engine) as session:
            return [
                appointment_json(session, item, current)
                for item in session.scalars(
                    select(Appointment)
                    .where(Appointment.student_id == current.id)
                    .order_by(Appointment.created_at.desc())
                )
            ]

    @app.get("/api/v1/appointments/{identifier}")
    def appointment(identifier: str, current: Auth):
        with Session(engine) as session:
            return appointment_json(session, appointment_access(session, current, identifier), current)

    @app.patch("/api/v1/appointments/{identifier}/cancel")
    def cancel(identifier: str, current: Auth):
        with transaction(engine) as session:
            item = appointment_access(session, current, identifier)
            if item.status not in ACTIVE:
                fail("Эту консультацию уже нельзя отменить")
            item.status = "cancelled"
            item.requested_slot_id = None
            item.previous_status = None
            reserve(session, item)
            record(session, item, current, "Консультация отменена")
        return {"status": "cancelled"}

    @app.post("/api/v1/appointments/{identifier}/reschedule")
    def reschedule(identifier: str, payload: Move, current: Auth):
        with transaction(engine) as session:
            item = appointment_access(session, current, identifier)
            if current.role != "student" or item.status not in {"pending", "confirmed"}:
                fail("Запрос переноса сейчас недоступен")
            if item.slot_id == payload.slot_id:
                fail("Выберите другое время")
            slot = available(session, payload.slot_id, item.specialist_id, item.id)
            check_student_time(session, item.student_id, slot, item.id)
            item.previous_status = item.status
            item.status = "reschedule"
            item.requested_slot_id = slot.id
            reserve(session, item)
            record(session, item, current, "Запрошен перенос консультации")
        return {"status": "reschedule"}

    @app.get("/api/v1/admin/appointments")
    def admin_appointments(
        current: Auth,
        q: str = Query(default="", max_length=160),
        status: str | None = None,
        day: date | None = None,
    ):
        if current.role == "student":
            fail("Нужна роль сотрудника", 403)
        with Session(engine) as session:
            query = (
                select(Appointment)
                .join(User, User.id == Appointment.student_id)
                .join(TimeSlot, TimeSlot.id == Appointment.slot_id)
            )
            if current.role != "admin":
                if not current.specialist_id:
                    fail("Сотрудник не назначен специалистом", 403)
                query = query.where(Appointment.specialist_id == current.specialist_id)
            if status:
                if status not in STATUSES:
                    fail("Неизвестный статус", 422)
                query = query.where(Appointment.status == status)
            if day:
                query = query.where(TimeSlot.day == day)
            items = session.scalars(query.order_by(TimeSlot.day.desc(), TimeSlot.time)).all()
            return [
                appointment_json(session, item, current)
                for item in items
                if not q
                or q.casefold()
                in (
                    session.get(User, item.student_id).name + " " + session.get(User, item.student_id).group
                ).casefold()
            ]

    @app.patch("/api/v1/admin/appointments/{identifier}")
    def decide(identifier: str, payload: Decision, current: Auth):
        with transaction(engine) as session:
            item = appointment_access(session, current, identifier, staff_only=True)
            moving = item.status == "reschedule"
            if payload.action == "complete":
                slot = get_item(session, TimeSlot, item.slot_id)
                if item.status != "confirmed" or slot_start(slot) + timedelta(minutes=slot.duration) > now():
                    fail("Завершить можно только состоявшуюся подтверждённую встречу")
                item.status = "completed"
                message = "Консультация завершена"
            else:
                if item.status not in {"pending", "reschedule"}:
                    fail("Заявка уже обработана")
                if payload.action == "approve":
                    slot = available(
                        session, item.requested_slot_id or item.slot_id, item.specialist_id, item.id
                    )
                    check_student_time(session, item.student_id, slot, item.id)
                    item.slot_id = slot.id
                    item.status = "confirmed"
                    message = "Перенос подтверждён" if moving else "Консультация подтверждена"
                else:
                    item.status = (item.previous_status or "confirmed") if moving else "rejected"
                    message = "Перенос отклонён, прежнее время сохранено" if moving else "Заявка отклонена"
                item.requested_slot_id = None
                item.previous_status = None
            reserve(session, item)
            record(session, item, current, message)
        return {"status": item.status}

    @app.post("/api/v1/admin/appointments/{identifier}/move")
    def move(identifier: str, payload: Move, current: Auth):
        with transaction(engine) as session:
            item = appointment_access(session, current, identifier, staff_only=True)
            if item.status not in ACTIVE or item.slot_id == payload.slot_id:
                fail("Эту запись нельзя перенести на указанное время")
            slot = available(session, payload.slot_id, item.specialist_id, item.id)
            check_student_time(session, item.student_id, slot, item.id)
            item.slot_id = slot.id
            item.requested_slot_id = None
            item.previous_status = None
            item.status = "confirmed"
            reserve(session, item)
            record(session, item, current, "Специалист перенёс консультацию")
        return {"status": "confirmed"}

    @app.patch("/api/v1/admin/appointments/{identifier}/notes")
    def notes(identifier: str, payload: Notes, current: Auth):
        with transaction(engine) as session:
            item = appointment_access(session, current, identifier, staff_only=True)
            item.internal_notes = payload.notes.strip()
            record(session, item, current, "Внутренняя заметка обновлена", internal=True, notify=False)
        return {"saved": True}

    @app.post("/api/v1/admin/specialists/{identifier}/slots", status_code=201)
    def create_slot(identifier: str, payload: SlotInput, current: Auth):
        with transaction(engine) as session:
            specialist_access(session, current, identifier)
            result = slot_json(add_slot(session, identifier, payload), False)
            audit(session, current, "Добавлен интервал", result["id"])
        return result

    @app.patch("/api/v1/admin/slots/{identifier}")
    def update_slot(identifier: str, payload: SlotInput, current: Auth):
        with transaction(engine) as session:
            slot = get_item(session, TimeSlot, identifier)
            specialist_access(session, current, slot.specialist_id)
            if session.get(Reservation, identifier):
                fail("Занятый интервал изменяется через перенос консультации")
            if slot.day == payload.day and slot.time == payload.time:
                return slot_json(slot, False)
            slot.hidden = True
            session.flush()
            result = slot_json(add_slot(session, slot.specialist_id, payload), False)
            audit(session, current, "Изменён интервал", identifier)
        return result

    @app.delete("/api/v1/admin/slots/{identifier}")
    def remove_slot(identifier: str, current: Auth):
        with transaction(engine) as session:
            slot = get_item(session, TimeSlot, identifier)
            specialist_access(session, current, slot.specialist_id)
            if session.get(Reservation, identifier):
                fail("Сначала перенесите или отмените консультацию")
            slot.hidden = True
            audit(session, current, "Убран интервал", identifier)
        return {"hidden": True}

    @app.post("/api/v1/admin/schedules", status_code=201)
    @app.patch("/api/v1/admin/schedules/{identifier}")
    def schedule(payload: ScheduleInput, current: Auth, identifier: str | None = None):
        if identifier is not None and identifier != payload.specialist_id:
            fail("Идентификатор расписания не совпадает со специалистом", 422)
        added = 0
        with transaction(engine) as session:
            specialist_access(session, current, payload.specialist_id)
            rules = payload.rules
            day = payload.from_day
            while day <= payload.until_day:
                if (day.weekday() + 1) % 7 in rules.weekdays:
                    cursor = datetime.combine(day, clock_time.fromisoformat(rules.start), MSK)
                    end = datetime.combine(day, clock_time.fromisoformat(rules.end), MSK)
                    break_from = datetime.combine(day, clock_time.fromisoformat(rules.breakStart), MSK)
                    break_to = datetime.combine(day, clock_time.fromisoformat(rules.breakEnd), MSK)
                    while cursor + timedelta(minutes=45) <= end:
                        if cursor > now() and not (
                            cursor < break_to and cursor + timedelta(minutes=45) > break_from
                        ):
                            existing = rows(
                                session, TimeSlot, specialist_id=payload.specialist_id, day=day, hidden=False
                            )
                            if not any(
                                cursor < slot_start(slot) + timedelta(minutes=slot.duration)
                                and slot_start(slot) < cursor + timedelta(minutes=45)
                                for slot in existing
                            ):
                                add_slot(
                                    session,
                                    payload.specialist_id,
                                    SlotInput(day=day, time=cursor.strftime("%H:%M")),
                                )
                                added += 1
                        cursor += timedelta(hours=1)
                day += timedelta(days=1)
            get_item(session, Schedule, payload.specialist_id).rules = rules.model_dump()
            audit(session, current, "Обновлено расписание", payload.specialist_id)
        return {"added": added}

    @app.delete("/api/v1/admin/specialists/{identifier}/days/{day}")
    def close_day(identifier: str, day: date, current: Auth):
        with transaction(engine) as session:
            specialist_access(session, current, identifier)
            slots_for_day = rows(session, TimeSlot, specialist_id=identifier, day=day)
            if any(session.get(Reservation, slot.id) for slot in slots_for_day):
                fail("На этот день есть записи. Сначала перенесите или отмените их")
            for slot in slots_for_day:
                slot.hidden = True
            audit(session, current, "Закрыт день", f"{identifier}:{day.isoformat()}")
        return {"closed": True}

    @app.get("/api/v1/notifications")
    def notifications(current: Auth):
        with Session(engine) as session:
            return notification_rows(session, current.id)

    @app.patch("/api/v1/notifications/read")
    def read_notifications(current: Auth):
        with transaction(engine) as session:
            for item in rows(session, Notification, student_id=current.id):
                item.read = True
        return {"read": True}

    @app.put("/api/v1/admin/users/{identifier}/role")
    def assign_role(identifier: str, payload: RoleInput, current: Auth):
        if current.role != "admin":
            fail("Только администратор может назначать роли", 403)
        with transaction(engine) as session:
            user = get_item(session, User, identifier)
            if identifier == current.id:
                fail("Собственную роль здесь менять нельзя", 403)
            user.role_id = payload.role
            audit(session, current, "Назначена роль " + payload.role, identifier)
        return {"role": payload.role}

    @app.put("/api/v1/admin/specialists/{identifier}/user")
    def assign_specialist(identifier: str, payload: Assignment, current: Auth):
        if current.role != "admin":
            fail("Только администратор может назначать специалистов", 403)
        with transaction(engine) as session:
            person = get_item(session, Specialist, identifier)
            user = get_item(session, User, payload.user_id)
            if user.role_id != identifier:
                fail("Роль пользователя должна соответствовать специалисту", 422)
            person.user_id = user.id
            audit(session, current, "Назначен сотрудник специалисту", identifier)
        return {"assigned": True}

    @app.middleware("http")
    async def response_headers(request: Request, call_next):
        if request.method not in {"GET", "HEAD", "OPTIONS"}:
            origin = request.headers.get("origin")
            if origin and origin.rstrip("/") != str(request.base_url).rstrip("/"):
                return JSONResponse({"detail": "Запрос с другого origin запрещён"}, status_code=403)
        response = await call_next(request)
        if request.url.path.startswith("/api/"):
            response.headers["Cache-Control"] = "no-store"
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Referrer-Policy"] = "same-origin"
        return response

    root = Path(__file__).parent
    frontend = Path(os.getenv("FRONTEND_DIR", str(root / "frontend")))

    @app.get("/", include_in_schema=False)
    def index():
        if (root / "index.html").is_file():
            return FileResponse(root / "index.html")
        return FileResponse(frontend / "index.html") if frontend.is_dir() else RedirectResponse("/docs")

    if frontend.is_dir():
        app.mount("/consultations", StaticFiles(directory=frontend, html=True), name="consultations")

        def asset_route(filename):
            def serve_asset():
                return FileResponse(frontend / filename)

            return serve_asset

        for filename in ("app.js", "api.js", "ui.js", "styles.css"):
            app.add_api_route("/" + filename, asset_route(filename), include_in_schema=False, methods=["GET"])
        app.mount("/assets", StaticFiles(directory=frontend / "assets"), name="assets")

    return app


if __name__ == "__main__":
    import uvicorn

    app = create_app()
    uvicorn.run(
        app,
        host="127.0.0.1" if app.state.local else os.getenv("HOST", "127.0.0.1"),
        port=int(os.getenv("PORT", "8000")),
        access_log=False,
    )
