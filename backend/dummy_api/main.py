import base64
import logging
from typing import Optional, List

from fastapi import Depends, FastAPI, HTTPException, status
from pydantic import BaseModel
from passlib.context import CryptContext
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
import jwt
import uvicorn
from jwt import PyJWTError
from sqlalchemy import (
    Column,
    Integer,
    String,
    ForeignKey,
    LargeBinary,
    create_engine,
    DateTime,
    Table,
)
from sqlalchemy.orm import sessionmaker, relationship, declarative_base, Session
from datetime import datetime, timedelta, timezone

# Конфигурация JWT
SECRET_KEY = "ваш_секретный_ключ_должен_быть_длинным_и_сложным"  # Замените на ваш секретный ключ
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,  # Уровень логирования (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",  # Формат логов
    handlers=[
        logging.StreamHandler(),  # Логи будут выводиться в консоль
        # Раскомментируйте следующую строку для записи логов в файл
        # logging.FileHandler("app.log"),
    ],
)
logger = logging.getLogger(__name__)

# Инициализация FastAPI
app = FastAPI()

# Хеширование паролей
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# Настройка базы данных SQLite с использованием SQLAlchemy
DATABASE_URL = "sqlite:///./app.db"

engine = create_engine(
    DATABASE_URL, connect_args={"check_same_thread": False}  # Только для SQLite
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Таблица ассоциации для связи многие-ко-многим между Packet и Tag
packet_tags = Table(
    "packet_tags",
    Base.metadata,
    Column("packet_id", Integer, ForeignKey("packets.id"), primary_key=True),
    Column("tag_id", Integer, ForeignKey("tags.id"), primary_key=True),
)

# ORM модели
class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    username = Column(String, unique=True, index=True, nullable=False)
    hashed_password = Column(String, nullable=False)


class StreamModel(Base):
    __tablename__ = "streams"

    id = Column(Integer, primary_key=True, index=True)
    src_ip = Column(String, nullable=False)
    src_port = Column(Integer, nullable=False)
    dest_ip = Column(String, nullable=False)
    dest_port = Column(Integer, nullable=False)

    packets = relationship("PacketModel", back_populates="stream")


class PacketModel(Base):
    __tablename__ = "packets"

    id = Column(Integer, primary_key=True, index=True)
    stream_id = Column(Integer, ForeignKey("streams.id"), nullable=False)
    timestamp = Column(DateTime, default=datetime.utcnow)
    origin = Column(String, nullable=False)
    data = Column(LargeBinary, nullable=False)

    stream = relationship("StreamModel", back_populates="packets")
    tags = relationship("Tag", secondary=packet_tags, back_populates="packets")


class Tag(Base):
    __tablename__ = "tags"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True, index=True, nullable=False)

    packets = relationship("PacketModel", secondary=packet_tags, back_populates="tags")


# Создание таблиц в базе данных
Base.metadata.create_all(bind=engine)

# Pydantic схемы
class Token(BaseModel):
    access_token: str
    token_type: str


class TokenData(BaseModel):
    username: Optional[str] = None


class UserRead(BaseModel):
    username: str

    class Config:
        orm_mode = True


class UserCreate(BaseModel):
    username: str
    password: str


class TagRead(BaseModel):
    name: str

    class Config:
        orm_mode = True


class PacketRead(BaseModel):
    id: int
    stream_id: int
    timestamp: datetime
    origin: str
    data_base64: str
    tags: List[TagRead] = []

    class Config:
        orm_mode = True


class StreamRead(BaseModel):
    id: int
    src_ip: str
    src_port: int
    dest_ip: str
    dest_port: int
    packets: List[PacketRead] = []

    class Config:
        orm_mode = True


class PacketCreate(BaseModel):
    stream_id: int
    origin: str
    data: bytes
    tags: Optional[List[str]] = []  # Список названий тегов


# Dependency для получения сессии базы данных
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# Настройка OAuth2
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


# Функции для работы с пользователями и токенами
def verify_password(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)


def get_password_hash(password: str) -> str:
    return pwd_context.hash(password)


def get_user(db: Session, username: str) -> Optional[User]:
    return db.query(User).filter(User.username == username).first()


def authenticate_user(db: Session, username: str, password: str) -> Optional[User]:
    user = get_user(db, username)
    if not user:
        logger.warning(f"Попытка аутентификации несуществующего пользователя: {username}")
        return None
    if not verify_password(password, user.hashed_password):
        logger.warning(f"Неверный пароль для пользователя: {username}")
        return None
    logger.info(f"Пользователь аутентифицирован: {username}")
    return user


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.now(timezone.utc) + expires_delta
    else:
        # По умолчанию токен действителен 15 минут
        expire = datetime.now(timezone.utc) + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    logger.debug(f"Создан JWT токен для пользователя: {data.get('sub')}")
    return encoded_jwt


async def get_current_user(
    token: str = Depends(oauth2_scheme), db: Session = Depends(get_db)
) -> User:
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Неверные учетные данные",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            logger.error("JWT токен не содержит поля 'sub'")
            raise credentials_exception
        token_data = TokenData(username=username)
    except PyJWTError as e:
        logger.error(f"Ошибка декодирования JWT токена: {e}")
        raise credentials_exception
    user = get_user(db, username=token_data.username)
    if user is None:
        logger.error(f"Пользователь из токена не найден: {token_data.username}")
        raise credentials_exception
    logger.info(f"Текущий аутентифицированный пользователь: {username}")
    return user


# Незащещённые маршруты
@app.get("/")
async def read_root():
    logger.info("Доступ к корневому маршруту")
    return {"message": "Добро пожаловать в API!"}


@app.get("/public-info/")
async def public_info():
    logger.info("Доступ к публичной информации")
    return {"info": "Это публичная информация, доступная без аутентификации."}


# Маршрут для регистрации нового пользователя (незащещённый)
@app.post("/register", response_model=UserRead)
async def register(user: UserCreate, db: Session = Depends(get_db)):
    logger.info(f"Попытка регистрации пользователя: {user.username}")
    db_user = get_user(db, username=user.username)
    if db_user:
        logger.warning(f"Регистрация не удалась: пользователь {user.username} уже существует")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Пользователь с таким именем уже существует",
        )
    hashed_password = get_password_hash(user.password)
    new_user = User(username=user.username, hashed_password=hashed_password)
    db.add(new_user)
    db.commit()
    db.refresh(new_user)
    logger.info(f"Пользователь зарегистрирован: {user.username}")
    return UserRead(username=new_user.username)


# Маршрут для получения токена (незащещённый)
@app.post("/token", response_model=Token)
async def login_for_access_token(
    form_data: OAuth2PasswordRequestForm = Depends(), db: Session = Depends(get_db)
):
    logger.info(f"Попытка логина пользователя: {form_data.username}")
    user = authenticate_user(db, form_data.username, form_data.password)
    if not user:
        logger.warning(f"Логин не удался для пользователя: {form_data.username}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Неверные учетные данные",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": user.username}, expires_delta=access_token_expires
    )
    logger.info(f"Токен выдан для пользователя: {user.username}")
    return {"access_token": access_token, "token_type": "bearer"}


# Защищённые маршруты
@app.get("/users/me/", response_model=UserRead)
async def read_users_me(current_user: User = Depends(get_current_user)):
    logger.info(f"Доступ к информации о пользователе: {current_user.username}")
    return UserRead(username=current_user.username)


@app.get("/protected-route/")
async def protected_route(current_user: User = Depends(get_current_user)):
    logger.info(
        f"Доступ к защищённому маршруту '/protected-route/' для пользователя: {current_user.username}"
    )
    return {"message": f"Привет, {current_user.username}! Это защищённый маршрут."}


@app.get("/another-protected/")
async def another_protected(current_user: User = Depends(get_current_user)):
    logger.info(
        f"Доступ к защищённому маршруту '/another-protected/' для пользователя: {current_user.username}"
    )
    return {"message": f"Доступ разрешён для {current_user.username}."}


# Маршрут для создания нового пакета с опциональными тегами
@app.post("/packets/", response_model=PacketRead)
async def create_packet(
    packet: PacketCreate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    logger.info(
        f"Пользователь {current_user.username} создает пакет для stream_id: {packet.stream_id}"
    )
    # Проверка существования стрима
    stream = db.query(StreamModel).filter(StreamModel.id == packet.stream_id).first()
    if not stream:
        logger.warning(
            f"Создание пакета не удалось: Stream с id {packet.stream_id} не найден"
        )
        raise HTTPException(status_code=404, detail="Stream не найден")

    # Создание нового пакета
    new_packet = PacketModel(
        stream_id=packet.stream_id, origin=packet.origin, data=packet.data
    )

    # Обработка тегов
    if packet.tags:
        tags = []
        for tag_name in packet.tags:
            tag = db.query(Tag).filter(Tag.name == tag_name).first()
            if not tag:
                # Создание нового тега, если он не существует
                tag = Tag(name=tag_name)
                db.add(tag)
                db.commit()
                db.refresh(tag)
                logger.info(f"Создан новый тег: {tag_name}")
            tags.append(tag)
        new_packet.tags = tags
        logger.info(
            f"Пакет с id {new_packet.id} получил теги: {[tag.name for tag in tags]}"
        )

    db.add(new_packet)
    db.commit()
    db.refresh(new_packet)

    # Подготовка ответа
    encoded_data = base64.b64encode(new_packet.data).decode("utf-8")
    tags_read = [TagRead(name=tag.name) for tag in new_packet.tags]
    logger.info(f"Пакет с id {new_packet.id} успешно создан")

    return PacketRead(
        id=new_packet.id,
        stream_id=new_packet.stream_id,
        timestamp=new_packet.timestamp,
        origin=new_packet.origin,
        data_base64=encoded_data,
        tags=tags_read,
    )


# Маршрут для получения всех пакетов, содержащих все заданные теги
@app.get("/packets/search/", response_model=List[PacketRead])
async def get_packets_by_tags(
    tags: List[str],
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    logger.info(
        f"Пользователь {current_user.username} ищет пакеты с тегами: {tags}"
    )
    if not tags:
        logger.warning("Поиск пакетов не удался: не предоставлены теги")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Необходимо предоставить список тегов для поиска",
        )

    # Получение объектов тегов из базы данных
    tag_objs = db.query(Tag).filter(Tag.name.in_(tags)).all()
    if len(tag_objs) != len(tags):
        missing_tags = set(tags) - set(tag.name for tag in tag_objs)
        logger.warning(f"Поиск пакетов не удался: теги не найдены: {missing_tags}")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Теги не найдены: {', '.join(missing_tags)}",
        )

    # Поиск пакетов, которые содержат все указанные теги
    query = db.query(PacketModel)
    for tag in tag_objs:
        query = query.filter(PacketModel.tags.contains(tag))
    packets = query.order_by(PacketModel.timestamp).all()

    # Подготовка ответа
    packets_read = []
    for packet in packets:
        encoded_data = base64.b64encode(packet.data).decode("utf-8")
        tags_read = [TagRead(name=tag.name) for tag in packet.tags]
        packets_read.append(
            PacketRead(
                id=packet.id,
                stream_id=packet.stream_id,
                timestamp=packet.timestamp,
                origin=packet.origin,
                data_base64=encoded_data,
                tags=tags_read,
            )
        )
        logger.debug(
            f"Пакет с id {packet.id} содержит теги: {[tag.name for tag in packet.tags]}"
        )

    logger.info(
        f"Найдено {len(packets_read)} пакетов, содержащих все заданные теги: {tags}"
    )
    return packets_read


# Защищённый маршрут для получения данных Stream в формате Base64 с пакетами
@app.get("/streams/{id}", response_model=StreamRead)
async def get_stream(
    id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    logger.info(f"Пользователь {current_user.username} запрашивает stream с ID: {id}")
    stream = db.query(StreamModel).filter(StreamModel.id == id).first()
    if not stream:
        logger.warning(
            f"Stream с ID {id} не найден для пользователя {current_user.username}"
        )
        raise HTTPException(status_code=404, detail="Stream не найден")

    # Получение и сортировка пакетов по timestamp
    packets = (
        db.query(PacketModel)
        .filter(PacketModel.stream_id == id)
        .order_by(PacketModel.timestamp)
        .all()
    )

    # Кодирование данных пакетов в Base64
    packets_read = []
    for packet in packets:
        encoded_data = base64.b64encode(packet.data).decode("utf-8")
        tags_read = [TagRead(name=tag.name) for tag in packet.tags]
        packets_read.append(
            PacketRead(
                id=packet.id,
                stream_id=packet.stream_id,
                timestamp=packet.timestamp,
                origin=packet.origin,
                data_base64=encoded_data,
                tags=tags_read,
            )
        )
        logger.debug(
            f"Packet с ID {packet.id} закодирован в Base64 для пользователя {current_user.username}"
        )

    # Создание Pydantic модели для стрима с пакетами
    stream_read = StreamRead(
        id=stream.id,
        src_ip=stream.src_ip,
        src_port=stream.src_port,
        dest_ip=stream.dest_ip,
        dest_port=stream.dest_port,
        packets=packets_read,
    )
    logger.debug(f"Stream с ID {id} обработан для пользователя {current_user.username}")
    return stream_read


# Функция для создания начальных данных (опционально)
def create_initial_data(db: Session):
    # Добавление начальных стримов, если их нет
    if db.query(StreamModel).count() == 0:
        logger.info("Добавление начальных данных в таблицу streams")
        stream1 = StreamModel(
            src_ip="192.168.1.1", src_port=12345, dest_ip="10.0.0.1", dest_port=80
        )
        stream2 = StreamModel(
            src_ip="192.168.1.2", src_port=23456, dest_ip="10.0.0.2", dest_port=443
        )
        db.add_all([stream1, stream2])
        db.commit()
        logger.info("Начальные стримы добавлены")
    else:
        logger.info("Начальные стримы уже существуют")

    # Добавление начальных пакетов, если их нет
    if db.query(PacketModel).count() == 0:
        logger.info("Добавление начальных данных в таблицу packets")
        packet1 = PacketModel(
            stream_id=1, origin="source", data=b"Packet data for ID 1"
        )
        packet2 = PacketModel(
            stream_id=1, origin="destination", data=b"Packet data for ID 2"
        )
        packet3 = PacketModel(
            stream_id=2, origin="source", data=b"Packet data for ID 3"
        )
        db.add_all([packet1, packet2, packet3])
        db.commit()
        logger.info("Начальные пакеты добавлены")
    else:
        logger.info("Начальные пакеты уже существуют")

    # Добавление начальных тегов и связывание их с пакетами (опционально)
    if db.query(Tag).count() == 0:
        logger.info("Добавление начальных тегов")
        tag1 = Tag(name="important")
        tag2 = Tag(name="review")
        db.add_all([tag1, tag2])
        db.commit()
        logger.info("Начальные теги добавлены")

    # Привязка тегов к пакетам (опционально)
    packet1 = db.query(PacketModel).filter(PacketModel.id == 1).first()
    packet2 = db.query(PacketModel).filter(PacketModel.id == 2).first()
    if packet1 and not packet1.tags:
        tag1 = db.query(Tag).filter(Tag.name == "important").first()
        if tag1:
            packet1.tags.append(tag1)
            logger.info(f"Пакету {packet1.id} присвоен тег '{tag1.name}'")
    if packet2 and not packet2.tags:
        tag2 = db.query(Tag).filter(Tag.name == "review").first()
        if tag2:
            packet2.tags.append(tag2)
            logger.info(f"Пакету {packet2.id} присвоен тег '{tag2.name}'")
    db.commit()


# Блок для запуска приложения как питоновского скрипта
if __name__ == "__main__":
    db = SessionLocal()
    # Проверка существования пользователя с id=1, если нет, создаём его
    user = db.query(User).filter(User.username == "johndoe").first()
    if not user:
        logger.info("Создание пользователя 'johndoe' для начальных данных")
        user = User(username="johndoe", hashed_password=pwd_context.hash("secret"))
        db.add(user)
        db.commit()
        logger.info("Пользователь 'johndoe' создан")
    create_initial_data(db)
    db.close()

    # Запуск сервера
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)
