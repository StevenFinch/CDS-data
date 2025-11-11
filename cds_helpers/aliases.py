# cds_helpers/aliases.py

def default_aliases_for_entity(user_entity: str):
    """
    Return a list of possible reference names for the same reference entity in SBSDR.
    We'll lowercase everything for comparison later.
    Extend this as needed.
    """
    base = user_entity.strip()
    aliases = {
        "united states of america": [
            "united states of america",
            "united states",
            "u.s. government",
            "us government",
            "u.s. sovereign",
            "usa",
            "u.s.a.",
        ],
    }

    key = base.lower()
    if key in aliases:
        return list(dict.fromkeys(aliases[key] + [key]))
    else:
        # fallback: just the provided one
        return [key]


def tenor_close_enough(request_years: float, effective_date, maturity_date, tol_years=1.0):
    """
    Rough tenor filter:
    compute (maturity - effective) in years and compare to request_years.
    We allow +- tol_years wiggle because real trades won't be exactly 5.00y.
    If missing dates, we can't judge, return True.
    """
    import datetime as dt
    if not (effective_date and maturity_date):
        return True  # can't disprove it
    if not (isinstance(effective_date, dt.date) and isinstance(maturity_date, dt.date)):
        return True
    days = (maturity_date - effective_date).days
    if days <= 0:
        return False
    yrs = days / 365.25
    return abs(yrs - request_years) <= tol_years
