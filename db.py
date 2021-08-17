import sqlalchemy as sq
from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.orm.attributes import InstrumentedAttribute

engine = create_engine('sqlite:///db.db')
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
    def __init__(self):
        try:
            Base.metadata.create_all(bind=engine)
            self.session = sessionmaker(bind=engine)()
        except:
            raise Exception('DB not ready')

    def save_character(self, character):
        character_data = {
            key: value
            for key, value in character.items()
            if key in CharacterModel().get_fields()
        }
        new_character = CharacterModel(**character_data)
        self.session.add(new_character)
        self.session.commit()
