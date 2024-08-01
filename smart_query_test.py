import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Base, Dataset, AssetClass, Region, User
from smart_query import smart_query


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
    connection = engine.connect()
    transaction = connection.begin()
    Session = sessionmaker(bind=connection)
    session = Session()

    yield session

    session.close()
    transaction.rollback()
    connection.close()


@pytest.fixture
def sample_data(dbsession):
    # Create Asset Classes
    commodities = AssetClass(name="Commodities")
    equities = AssetClass(name="Equities")
    real_estate = AssetClass(name="Real Estate")

    # Create Regions
    north_america = Region(name="North America")
    us = Region(name="US", parent=north_america)
    canada = Region(name="Canada", parent=north_america)
    europe = Region(name="Europe")

    # Create Users
    user1 = User(username="user1")
    user2 = User(username="user2")

    # Create Datasets
    datasets = [
        Dataset(
            name="US Commodities",
            description="Commodity prices in the US",
            asset_classes=[commodities],
            regions=[us],
            maintainers=[user1],
        ),
        Dataset(
            name="Global Equities",
            description="World-wide stock prices",
            asset_classes=[equities],
            regions=[north_america, europe],
            maintainers=[user2],
        ),
        Dataset(
            name="NA Real Estate",
            description="Real estate data for North America",
            asset_classes=[real_estate],
            regions=[us, canada],
            maintainers=[user1],
        ),
        Dataset(
            name="Mixed Assets",
            description="Various asset types",
            asset_classes=[commodities, equities],
            regions=[north_america],
            maintainers=[user1, user2],
        ),
    ]

    dbsession.add_all(
        [
            commodities,
            equities,
            real_estate,
            north_america,
            us,
            canada,
            europe,
            user1,
            user2,
        ]
        + datasets
    )
    dbsession.commit()

    return datasets


def test_simple_equality(dbsession, sample_data):
    result = smart_query(dbsession, Dataset, ["name", "=", "US Commodities"]).all()
    assert len(result) == 1
    assert result[0].name == "US Commodities"


def test_like_operator(dbsession, sample_data):
    result = smart_query(
        dbsession, Dataset, ["description", "like", "%North America%"]
    ).all()
    assert len(result) == 1
    assert result[0].name == "NA Real Estate"


def test_in_operator(dbsession, sample_data):
    result = smart_query(
        dbsession, Dataset, ["name", "in", ["US Commodities", "Global Equities"]]
    ).all()
    assert len(result) == 2
    assert set(d.name for d in result) == {"US Commodities", "Global Equities"}


def test_relationship_query(dbsession, sample_data):
    result = smart_query(
        dbsession, Dataset, ["asset_classes.name", "=", "Commodities"]
    ).all()
    assert len(result) == 2
    assert set(d.name for d in result) == {"US Commodities", "Mixed Assets"}


def test_nested_relationship_query(dbsession, sample_data):
    result = smart_query(
        dbsession, Dataset, ["regions.parent.name", "=", "North America"]
    ).all()
    assert len(result) == 2
    assert set(d.name for d in result) == {"US Commodities", "NA Real Estate"}


def test_multiple_conditions(dbsession, sample_data):
    query_params = [
        ["asset_classes.name", "=", "Real Estate"],
        ["regions.name", "=", "US"],
    ]
    result = smart_query(dbsession, Dataset, query_params).all()
    assert len(result) == 1
    assert result[0].name == "NA Real Estate"


def test_or_condition(dbsession, sample_data):
    query_params = [
        "OR",
        ["name", "=", "US Commodities"],
        ["name", "=", "Global Equities"],
    ]
    result = smart_query(dbsession, Dataset, query_params).all()
    assert set(d.name for d in result) == {"US Commodities", "Global Equities"}
    assert len(result) == 2


def test_not_condition(dbsession, sample_data):
    result = smart_query(
        dbsession, Dataset, ["NOT", ["asset_classes.name", "=", "Commodities"]]
    ).all()
    # assert len(result) == 2
    assert set(d.name for d in result) == {"Global Equities", "NA Real Estate"}


def test_complex_query(dbsession, sample_data):
    query_params = [
        ["regions.name", "=", "North America"],
        ["NOT", ["asset_classes.name", "=", "Real Estate"]],
        [
            "OR",
            ["maintainers.username", "=", "user1"],
            ["maintainers.username", "=", "user2"],
        ],
    ]
    result = smart_query(dbsession, Dataset, query_params).all()
    assert set(d.name for d in result) == {"Global Equities", "Mixed Assets"}
    assert len(result) == 2


if __name__ == "__main__":
    pytest.main()
