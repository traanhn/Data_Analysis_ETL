"""
Usage: Data analysis part
    Calculate:
    - default radius over timeframe provided
    - total hours of radius reduction
Author: traanhn 2021-09-22
"""

from datetime import timedelta
import pandas as pd
from etl import db_connection  # Import from local source code
from global_vars import *  # Import global variables


def select_sql_query(table_name):
    """
    This function input the table name in db, fetched from corresponding csv filename and return sql query for selection
    :param table_name: table name from sql db
    :return: sql select query
    """
    query = "select * from {} ".format(table_name)
    return query


def dataframe_from_sql_query(conn, table_name):
    """
    Convert sql table into DataFrame format
    :param table_name: the name of the sql table in the database. This is the same name as the source csv files
    :param conn: db connection
    :return: DataFrame table
    """
    query = select_sql_query(table_name)
    df = pd.read_sql_query(query, conn)
    return df


def event_period_calc(df_source, event_timestamp, period=None):
    """
    Calculate the corresponding period, specified from input parameter for each row in dataframe, which has timestamp
    data
    :param df_source: DataFrame which has timestamp column
    :param event_timestamp: the timestamp column in DataFrame
    :param period: could be month, weekday, hour string values. Default = None
    :return: DataFrame which has 1 additional column , event_period, which displays the corresponding calculated period
    for the event_timestamp and the specified period value
    """
    # Extracting the date part from the event started timestamp
    if period == 'month':
        df_source[event_period] = pd.DatetimeIndex(df_source[event_timestamp]).month
    elif period == 'weekday':
        df_source[event_period] = pd.DatetimeIndex(df_source[event_timestamp]).weekday
    elif period == 'hour':
        df_source[event_period] = pd.DatetimeIndex(df_source[event_timestamp]).hour
    elif period == 'dayofyear':
        df_source[event_period] = pd.DatetimeIndex(df_source[event_timestamp]).dayofyear
    elif period is None:
        df_source[event_period] = pd.DatetimeIndex(df_source[event_timestamp])
    else:
        df_source[event_period] = pd.DatetimeIndex(df_source[event_timestamp])
    return df_source


def formatted_radius_source_data(df_source, period=None):
    """
    Formatting the radius source data. Convert object type to correct datetime and float formats.
    Adding artificial data rows 'missing' values in the dataset. This is for the radius calculation by period
    :param period: period: could be month, weekday, hour string values. Default = None
    :param df_source: delivery radius DataFrame
    :return: delivery radius DataFrame with an expected format
    """
    # Convert the data types
    df_source[event_started_timestamp] = df_source[event_started_timestamp].astype('datetime64[ns]')
    df_source[delivery_radius_meters] = pd.to_numeric(df_source[delivery_radius_meters])
    # Union the source data with the artificial time dataframe at hourly level, for specified date range
    df_source = pd.concat([df_source, pd.DataFrame(pd.date_range(start_date, end_date, freq="H"),
                                                   columns=[event_started_timestamp])], axis=0,
                          ignore_index=True). \
        drop_duplicates(subset=event_started_timestamp, keep='first'). \
        sort_values(by=[event_started_timestamp]).reset_index(drop=True)
    # Manipulate the delivery radius to get the next row value for delivery radius,
    # for artificial rows added in the previous step
    df_source = df_source.fillna(method='ffill')
    # calculate the period for default radius calculations
    df_source = event_period_calc(df_source, event_started_timestamp, period)
    return df_source


def formatted_purchases_source_data(df_source, period=None):
    """
    Formatting the purchase source data. Convert object type to correct datetime and float formats.
    Sorting the data by time order. Calculate the corresponding period for each event timestamp
    :param period: could be month, weekday, hour string values. Default = None
    :param df_source: purchase DataFrame
    :return: purchase DataFrame with an expected format
    """
    # Convert the data types
    df_source[time_received] = df_source[time_received].astype('datetime64[ns]')
    df_source[time_delivered] = df_source[time_delivered].astype('datetime64[ns]')
    df_source[delivery_amount] = pd.to_numeric(df_source[delivery_amount])
    df_source[distance] = pd.to_numeric(df_source[distance])
    df_source = df_source.sort_values(by=[time_received]).reset_index(drop=True)
    # calculate the period for default radius calculations #
    df_source = event_period_calc(df_source, time_received, period)
    return df_source


def delivery_radius_event_duration(df_source, period=None):
    """
    Calculate the duration of each event for delivery radius change. The duration is calculated in hour.
    :param df_source: delivery radius DataFrame
    :param period: could be month, weekday, hour string values. Default = None
    :return: DataFrame which has additional column duration
    """
    # Transforming the datatype into the correct formats
    df_source = formatted_radius_source_data(df_source, period)
    # Calculate the duration of each event
    df_source[event_ended_timestamp] = df_source[event_started_timestamp].shift(-1) - timedelta(
        microseconds=1000)
    df_source[duration] = df_source[event_ended_timestamp] - df_source[event_started_timestamp]
    # Convert duration to hours format
    df_source[duration] = df_source[duration].astype('timedelta64[s]') / 3600
    return df_source


def default_radius_calculation(df_source, period=None):
    """
    Calculate the default radius for specified period
    :param df_source:
    :param period: could be 'month', 'weekday', 'hour' string values. Default = None
    :return: DataFrame illustrates the default radius by specified period
    """
    # Extracting the date part from the event started timestamp
    df_event_duration = delivery_radius_event_duration(df_source, period)
    # aggregate the duration of event by event period and delivery radius
    sum_duration_df = df_event_duration.groupby([event_period, delivery_radius_meters]).agg(
        {duration: 'sum'})
    # get the default radius by period by, which has the highest total duration by such period
    df_default_radius_by_period = sum_duration_df.groupby(level=0).max().reset_index().merge(sum_duration_df. \
                                                                                             reset_index())
    return df_default_radius_by_period


def total_hours_radius_reduction(df_source, period=None):
    """
    Calculate the total hours of radius reduction by comparing against the default radius calculated by period
    :param df_source: delivery radius DataFrame
    :param period:  could be 'month', 'weekday', 'hour' string values. Default = None
    :return: the total hours and DataFrame displays the events which experience the radius reduction, compared against
    the default radius for corresponding periods
    """

    df_event_duration = delivery_radius_event_duration(df_source, period)
    df_default_radius = default_radius_calculation(df_source, period)
    # left join the processed source data to the calculated default radius dataframe
    df = df_event_duration.merge(df_default_radius, how='left', on=event_period, suffixes=('', '_DEFAULT'))
    # Check if event has a reduction in delivery radius, compared to the pre-defined default radius
    df_radius_reduction_periods = df[df[delivery_radius_meters] < df[delivery_radius_meters + '_DEFAULT']]
    # Calculate the total hours of experiencing the reduction in radius
    total_hours = df_radius_reduction_periods[duration].sum(skipna=True)
    return total_hours, df_radius_reduction_periods


def total_hours_radius_reduction_original_source(df_source):
    """
    Calculate the total hours of radius reduction by compared among events themselves, not using default radius for this
    :param df_source: delivery radius DataFrame
    :return: total hours of radius reduction and DataFrame which displays events with smaller radius values compared to
    the previous row.
    """
    df_event_duration = delivery_radius_event_duration(df_source)
    # Checking if the radius of next row is smaller than the radius of the previous row
    df_radius_reduction = df_event_duration[
        df_event_duration[delivery_radius_meters] < df_event_duration[delivery_radius_meters].shift(1)]
    # Sum up the duration from the filtered dataframe to get the total hours of radius reduction
    total_hours = df_radius_reduction[duration].sum(skipna=True)
    return total_hours, df_radius_reduction


def potential_loss_revenue(df_purchase, df_radius, period=None):
    """
    Using the default radius by period to check if a potential purchase might be missed if the drop off distance is
    larger than the corresponding default radius
    :param df_purchase: purchase DataFrame
    :param df_radius : delivery radius DataFrame
    :param period: could be 'month', 'weekday', 'hour' string values. Default = None
    :return: total potential revenue loss in EUR amount and DataFrame for potential loss purchases
    """
    df_purchase = formatted_purchases_source_data(df_purchase, period)
    df_radius_reduction = default_radius_calculation(df_radius, period)
    df_purchase = df_purchase.merge(df_radius_reduction, how='left', on=event_period)
    # Checking cases where the drop off distance is larger than the default radius for corresponding period
    df_purchase = df_purchase[df_purchase[distance] > df_purchase[delivery_radius_meters]]
    loss_revenue = df_purchase[delivery_amount].sum(skipna=True)
    return loss_revenue, df_purchase


def display_result_summary(df_radius, df_purchase, period=None):
    """
    Display the final results in readable formats
    :param df_purchase: purchase DataFrame
    :param df_radius : delivery radius DataFrame
    :param period: could be 'month', 'weekday', 'hour' string values. Default = None
    """
    df_default_radius = default_radius_calculation(df_radius, period)
    total_hours_by_period, df_radius_reduction = total_hours_radius_reduction(df_radius, period)
    loss_revenue, df_potential_loss_purchases = potential_loss_revenue(df_purchase, df_radius, period)
    print('Default radius of a given period by', period, ' :\n', df_default_radius)
    print('Total hours of reduction over the given timeframe: ', total_hours_by_period)
    print('The potential loss of revenue over the given timeframe (in EUR) : ', loss_revenue)


def main():
    cur, conn = db_connection()
    period = 'dayofyearhour'
    df_source_radius = dataframe_from_sql_query(conn, delivery_radius_log)
    df_source_purchases = dataframe_from_sql_query(conn, purchases)
    display_result_summary(df_source_radius, df_source_purchases, period)


if __name__ == '__main__':
    main()
