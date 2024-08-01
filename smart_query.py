import logging
from sqlalchemy import inspect, or_, and_, not_, exists, select
from sqlalchemy.orm import aliased
from sqlalchemy.sql import operators

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def simple_op(op, column, value):
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
    return condition


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
                inner_alias = aliased(model)
                subquery = select(1).select_from(inner_alias)
                for i, part in enumerate(parts[:-1]):
                    relationship = getattr(current_model, part)
                    related_model = relationship.property.mapper.class_

                    if related_model == current_model:
                        # Self-referential relationship
                        alias = aliased(related_model)
                        subquery = subquery.join(alias, relationship)
                        current_model = alias
                    else:
                        if i == 0:
                            subquery = subquery.join(relationship)
                        else:
                            subquery = subquery.join(relationship)
                        current_model = related_model
                field = parts[-1]
                column = getattr(current_model, field)

                # Add correlation condition
                correlation_condition = model.id == inner_alias.id
            else:
                column = getattr(model, field)
                condition = simple_op(op, column, value)
                return ~condition if is_not else condition

            condition = simple_op(op, column, value)

            subquery = subquery.where(and_(condition, correlation_condition))

            if is_not:
                return ~exists(subquery)
            else:
                return exists(subquery)

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
