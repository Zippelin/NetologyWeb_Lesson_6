import sqlalchemy as sq
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.orm.attributes import InstrumentedAttribute

engine = create_async_engine('sqlite+aiosqlite:///db.db', echo=True)
Base = declarative_base()


class CharacterModel(Base):
    __tablename__ = 'sw_characters'

    id = sq.Column(sq.INTEGER, primary_key=True)
    birth_year = sq.Column(sq.String)
    eye_color = sq.Column(sq.String)
    films = sq.Column(sq.String)
    gender = sq.Column(sq.String)
    hair_color = sq.Column(sq.String)
    height = sq.Column(sq.String)
    homeworld = sq.Column(sq.String)
    mass = sq.Column(sq.String)
    name = sq.Column(sq.String)
    skin_color = sq.Column(sq.String)
    species = sq.Column(sq.String)
    starships = sq.Column(sq.String)
    vehicles = sq.Column(sq.String)

    @classmethod
    def get_fields(cls):
        return [column for column in cls.__dict__ if isinstance(cls.__dict__[column], InstrumentedAttribute)]


class DBWorker:

    async def begin(self):
        try:
            async with engine.begin() as conn:
                await conn.run_sync(Base.metadata.drop_all)
                await conn.run_sync(Base.metadata.create_all)
            self.async_session = sessionmaker(bind=engine, expire_on_commit=False, class_=AsyncSession)
        except:
            raise Exception('DB not ready')

    async def save_character(self, character):
        character_data = {
            key: value
            for key, value in character.items()
            if key in CharacterModel().get_fields()
        }
        async with self.async_session() as session:
            async with session.begin():
                session.add(CharacterModel(**character_data))

            await session.commit()


async def dispose_engine():
    await engine.dispose()