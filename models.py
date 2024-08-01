from sqlalchemy import Column, Integer, String, ForeignKey, Table, select
from sqlalchemy.orm import relationship, DeclarativeBase
from sqlalchemy.sql import func


class Base(DeclarativeBase):
    pass


# Association tables for many-to-many relationships
dataset_asset_class = Table(
    "dataset_asset_class",
    Base.metadata,
    Column("dataset_id", Integer, ForeignKey("datasets.id")),
    Column("asset_class_id", Integer, ForeignKey("asset_classes.id")),
)

dataset_region = Table(
    "dataset_region",
    Base.metadata,
    Column("dataset_id", Integer, ForeignKey("datasets.id")),
    Column("region_id", Integer, ForeignKey("regions.id")),
)

dataset_maintainer = Table(
    "dataset_maintainer",
    Base.metadata,
    Column("dataset_id", Integer, ForeignKey("datasets.id")),
    Column("user_id", Integer, ForeignKey("users.id")),
)


class AssetClass(Base):
    __tablename__ = "asset_classes"

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)


class Region(Base):
    __tablename__ = "regions"

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    parent_id = Column(Integer, ForeignKey("regions.id"))

    parent = relationship("Region", remote_side=[id], back_populates="children")
    children = relationship("Region", back_populates="parent")

    @classmethod
    def get_all_subregions(cls, session, parent_name):
        cte = (
            select(
                cls.id, cls.name, cls.parent_id, func.cast(1, Integer).label("level")
            )
            .where(cls.name == parent_name)
            .cte(name="region_cte", recursive=True)
        )

        cte_alias = cte.alias()
        region_alias = cls.__table__.alias()

        cte = cte.union_all(
            select(
                region_alias.c.id,
                region_alias.c.name,
                region_alias.c.parent_id,
                (cte_alias.c.level + 1).label("level"),
            ).where(region_alias.c.parent_id == cte_alias.c.id)
        )

        query = select(cte.c.id, cte.c.name, cte.c.parent_id, cte.c.level).order_by(
            cte.c.level
        )

        return session.execute(query).fetchall()


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True)
    username = Column(String, nullable=False, unique=True)


class Dataset(Base):
    __tablename__ = "datasets"

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    description = Column(String)

    asset_classes = relationship("AssetClass", secondary=dataset_asset_class)
    regions = relationship("Region", secondary=dataset_region)
    maintainers = relationship("User", secondary=dataset_maintainer)
