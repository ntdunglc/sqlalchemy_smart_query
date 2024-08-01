import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Base, AssetClass, Region, User, Dataset


@pytest.fixture(scope="module")
def engine():
    return create_engine("sqlite:///:memory:")


@pytest.fixture(scope="module")
def tables(engine):
    Base.metadata.create_all(engine)
    yield
    Base.metadata.drop_all(engine)


@pytest.fixture
def dbsession(engine, tables):
    """Returns an sqlalchemy session, and after the test tears down everything properly."""
    connection = engine.connect()
    transaction = connection.begin()
    Session = sessionmaker(bind=connection)
    session = Session()

    yield session

    session.close()
    transaction.rollback()
    connection.close()


def test_asset_class_crud(dbsession):
    # Create
    asset_class = AssetClass(name="Equities")
    dbsession.add(asset_class)
    dbsession.commit()

    # Read
    assert dbsession.query(AssetClass).filter_by(name="Equities").first() is not None

    # Update
    asset_class.name = "Fixed Income"
    dbsession.commit()
    assert (
        dbsession.query(AssetClass).filter_by(name="Fixed Income").first() is not None
    )

    # Delete
    dbsession.delete(asset_class)
    dbsession.commit()
    assert dbsession.query(AssetClass).filter_by(name="Fixed Income").first() is None


def test_region_hierarchy(dbsession):
    parent_region = Region(name="North America")
    child_region = Region(name="United States", parent=parent_region)
    state_region = Region(name="NY", parent=child_region)
    dbsession.add_all([parent_region, child_region, state_region])
    dbsession.commit()

    assert child_region.parent == parent_region
    assert parent_region.children[0] == child_region


def test_get_all_subregions(dbsession):
    # Create the region hierarchy
    north_america = Region(name="North America")
    us = Region(name="US", parent=north_america)
    canada = Region(name="Canada", parent=north_america)
    ny = Region(name="NY", parent=us)
    ca = Region(name="CA", parent=us)
    on = Region(name="Ontario", parent=canada)
    qc = Region(name="Quebec", parent=canada)

    dbsession.add_all([north_america, us, canada, ny, ca, on, qc])
    dbsession.commit()

    # Test getting all subregions of North America
    subregions = Region.get_all_subregions(dbsession, "North America")

    # Assertions
    assert len(subregions) == 7  # Including North America itself
    assert set(r.name for r in subregions) == {
        "North America",
        "US",
        "Canada",
        "NY",
        "CA",
        "Ontario",
        "Quebec",
    }

    # Verify the levels
    levels = {r.name: r.level for r in subregions}
    assert levels == {
        "North America": 1,
        "US": 2,
        "Canada": 2,
        "NY": 3,
        "CA": 3,
        "Ontario": 3,
        "Quebec": 3,
    }

    # Test getting all subregions of US
    us_subregions = Region.get_all_subregions(dbsession, "US")
    assert len(us_subregions) == 3  # Including US itself
    assert set(r.name for r in us_subregions) == {"US", "NY", "CA"}

    # Print the hierarchy
    print("Region hierarchy:")
    for region in subregions:
        print(f"{'  ' * (region.level - 1)}- {region.name}")


def test_user_creation(dbsession):
    user = User(username="john_doe")
    dbsession.add(user)
    dbsession.commit()

    assert dbsession.query(User).filter_by(username="john_doe").first() is not None


def test_dataset_relationships(dbsession):
    asset_class = AssetClass(name="Commodities")
    region = Region(name="Europe")
    user = User(username="jane_smith")
    dataset = Dataset(
        name="Commodity Prices",
        description="Historical commodity prices in Europe",
        asset_classes=[asset_class],
        regions=[region],
        maintainers=[user],
    )

    dbsession.add(dataset)
    dbsession.commit()

    retrieved_dataset = (
        dbsession.query(Dataset).filter_by(name="Commodity Prices").first()
    )
    assert retrieved_dataset is not None
    assert retrieved_dataset.asset_classes[0].name == "Commodities"
    assert retrieved_dataset.regions[0].name == "Europe"
    assert retrieved_dataset.maintainers[0].username == "jane_smith"


def test_unique_username(dbsession):
    user1 = User(username="unique_user")
    dbsession.add(user1)
    dbsession.commit()

    user2 = User(username="unique_user")
    dbsession.add(user2)

    with pytest.raises(Exception):  # This will catch any SQLAlchemy integrity errors
        dbsession.commit()


if __name__ == "__main__":
    pytest.main()
