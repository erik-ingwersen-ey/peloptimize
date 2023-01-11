""" Limits for the solver variables """
from typing import Dict, Hashable, Tuple
from pelopt.utils.logging_config import logger
from pelopt.utils.operations import normalize_feature, retrieve_dataset


def limits_by_minmax(limits, feature, query_range):
    values = limits[feature][query_range]
    return values.min(), values.max()


def limits_by_norm(feature, norm_value_one, norm_value_two=None):
    limit_one = normalize_var(feature, norm_value_one)
    if norm_value_two:
        return limit_one, normalize_var(feature, norm_value_two)
    return limit_one


def limits_by_quantile(
    limits, feature, query_range, quant_value_one, quant_value_two=None
):
    limit = limits[feature][query_range]
    if quant_value_two:
        return limit.quantile(quant_value_one), limit.quantile(quant_value_two)
    return limit.quantile(quant_value_one)


def limits_by_rolling_mean(df, scalers, feature, rolling, quant_one, quant_two):
    roll_mean = df.rolling(rolling).mean().quantile
    min_limit = roll_mean(quant_one) - scalers[feature].data_min_[0]
    max_limit = roll_mean(quant_two) - scalers[feature].data_min_[0]
    range_value = scalers[feature].data_range_[0]
    return min_limit / range_value, max_limit / range_value


def parse_limits(limits: Dict) -> Dict:
    """Parse limits from json file.

    Some tags reference the limits from other tags. Therefore, function loops
    through the limits, replacing these referenced limits with their real
    values.

    Parameters
    ----------
    limits : Dict
        Dictionary with each tag limits. Some tags limits
        refer to other tags, that get replaced by
        the tag that's being referenced limits.

    Returns
    -------
    Dict
        The limits of each tag, after replacing values that reference other
        tags by their actual limits.
    """
    for tag, value in limits.items():
        if isinstance(value, str):
            if value in limits.keys():
                limits[tag] = limits[value]
            else:
                logger.warning(
                    f"Tag %s references %s, which is not a valid tag.",
                    tag, value
                )
    return limits


def read_limits(file, feature=None):
    """Read and return the limits values."""
    limits = parse_limits(file)

    # In this case, the operation applied over that tag is different
    if isinstance(feature, Hashable) and not feature in limits.keys():
        return None
    return limits


def define_limit_by_quantile(
    feature: str,
    models_features: Dict,
    production_query: str,
    quantile_limits: Dict,
) -> Tuple:
    """Define feature limits, by quantile.

    Parameters
    ----------
    feature : str
        The name of the feature to define.
    models_features : Dict
        Dictionary with feature name as key, and an array as value.
    production_query : str
    quantile_limits : Dict
        Dictionary with feature name as key, and its limits as values.

    Returns
    -------
    Tuple
        The lower and upper bounds for the feature specified
        by the parameter :param:`feature`. If :param:`quantile_limits`
        equals an empty dictionary, or ``None``, then it returns a tuple
        of ``None``.
    """
    limits = quantile_limits
    if not limits:
        logger.warning(
            "`quantile_limits` not defined, returning `None, None` as limits "
            "for feature %s.", feature
        )
        return None, None

    feature_quantile = models_features[feature][production_query].quantile

    if "min" in limits[feature].keys() and "max" in limits[feature].keys():
        min_value, max_value = limits[feature]["min"], limits[feature]["max"]
        return feature_quantile(min_value), feature_quantile(max_value)
    if "min" in limits[feature].keys():
        logger.warning(
            "Feature %s has a minimum limit, but no maximum limit defined.",
            feature
        )
        return feature_quantile(limits[feature]["min"]), None
    if "max" in limits[feature].keys():
        logger.warning(
            "Feature %s has a maximum limit, but no minimum limit defined.",
            feature
        )
        return None, feature_quantile(limits[feature]["max"])
    logger.warning(
        "Found limits for feature '%s', but no `min` or `max` keys were "
        "found. Returning `None, None` as limits. Limits: %s",
        feature, limits[feature]
    )
    return None, None


def define_limit_by_normalization(scalers, feature, limits):
    """Define limits min and max by var normalization

    Returns
    -------
    Tuple
        Returns both values when lmin and lmax are defined.
        Otherwise, return the left value when only lmin is defined.
        Finally, it returns the right value when only lmax is defined.
    """
    arguments = {"scalers": scalers, "feature": feature}

    if len(limits[feature]) == 2:
        arg_min = {**arguments, "norm_value": limits[feature]["min"]}
        arg_max = {**arguments, "norm_value": limits[feature]["max"]}
        return normalize_feature(**arg_min), normalize_feature(**arg_max)

    # In this case has only one of the values
    # getting the min or max value in the limits
    if "min" in limits[feature].keys():
        args = {**arguments, "norm_value": limits[feature]["min"]}
        return normalize_feature(**args), None

    args = {**arguments, "norm_value": limits[feature]["max"]}
    return None, normalize_feature(**args)


def define_work_dataset(
    feature, datasets, production_query, status="status", status_add=False, dataset=None
):
    # In this case the dataset is not specified yet
    if not dataset:
        dataset = retrieve_dataset(feature, datasets)[0]

    production_query = production_query.rename("production")
    df = (
        datasets[dataset]
        .join(production_query)
        .loc[lambda xdf: xdf["production"] == True, :]
    )
    if status_add:
        return df[df[status + "_" + dataset] == 1][feature]
    return df[df[status] == 1][feature]


def define_limit_by_rolling_mean(
    feature, production_query, datasets, scalers, rolling_limits, other_feature=None
):
    arguments = {"status": "status", "status_add": False, "dataset": None}
    for special_feature in arguments.keys():
        temp_feature = feature
        if other_feature:
            temp_feature = other_feature
        if special_feature in rolling_limits[temp_feature].keys():
            arguments[special_feature] = rolling_limits[temp_feature][special_feature]
    df = define_work_dataset(feature, datasets, production_query, **arguments)
    return limits_by_rolling_mean(
        df,
        scalers,
        feature,
        rolling_limits[feature]["rolling"],
        rolling_limits[feature]["quant_one"],
        rolling_limits[feature]["quant_two"],
    )


def define_bentonita_limit(feature, datasets, production_query, scalers):
    df = define_work_dataset(feature, datasets, production_query)
    lmin, _ = limits_by_rolling_mean(df, scalers, feature, 24, 0.25, 0.25)
    aux = (
        df.rolling(24).mean().quantile(0.25)
        + df.rolling(24).mean().quantile(0.25) * 0.05
    )
    aux = max(aux, 0.0053)
    lmax = normalize_feature(scalers, feature, aux)
    return lmin, lmax


def define_constant_limits(feature, limits):
    logger.debug(limits)
    for key in limits.keys():
        if feature.startswith(key):
            feature = key
            break
    return limits[feature]["min"], limits[feature]["max"]
