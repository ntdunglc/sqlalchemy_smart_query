import logging
from sqlalchemy import inspect, or_, and_, not_, exists
from sqlalchemy.orm import aliased
from sqlalchemy.sql import operators

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def smart_query(session, model, query_params):
    query = session.query(model)

    def apply_condition(condition, is_not=False):
        if isinstance(condition, list) and condition[0] in ["AND", "OR", "NOT"]:
            op = condition[0]
            if op == "NOT":
                return apply_condition(condition[1], is_not=True)
            else:
                conditions = [apply_condition(c, is_not) for c in condition[1:]]
                return or_(*conditions) if op == "OR" else and_(*conditions)
        else:
            field, op, value = condition
            if "." in field:
                parts = field.split(".")
                current_model = model
                subquery = session.query(model.id).distinct()
                for i, part in enumerate(parts[:-1]):
                    relationship = getattr(current_model, part)
                    related_model = relationship.property.mapper.class_
                    if related_model == current_model:
                        # Self-referential relationship
                        alias = aliased(related_model)
                        subquery = subquery.join(alias, relationship)
                        current_model = alias
                    else:
                        subquery = subquery.join(relationship)
                        current_model = related_model
                field = parts[-1]
                column = getattr(current_model, field)
            else:
                column = getattr(model, field)
                subquery = session.query(model.id)

            if op == "=":
                condition = column == value
            elif op == "like":
                condition = column.like(value)
            elif op == "in":
                condition = column.in_(value)
            elif op == ">":
                condition = column > value
            elif op == "<":
                condition = column < value
            elif op == ">=":
                condition = column >= value
            elif op == "<=":
                condition = column <= value
            elif op == "!=":
                condition = column != value
            else:
                raise ValueError(f"Unsupported operator: {op}")

            subquery = subquery.filter(condition)

            if is_not:
                return ~model.id.in_(subquery)
            else:
                return model.id.in_(subquery)

    if isinstance(query_params[0], list):
        # Multiple conditions or advanced query
        final_condition = apply_condition(["AND"] + query_params)
    else:
        # Single condition
        final_condition = apply_condition(query_params)

    query = query.filter(final_condition)

    # Log the generated SQL query
    logger.info(f"Generated SQL: {query}")

    return query
