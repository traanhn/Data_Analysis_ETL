"""
Usage: Store global variables for the project
Author: Tra-Anh Nguyen 2021-09-12
"""

#  Static names of sql tables
delivery_radius_log = "delivery_radius_log"
purchases = "purchases"

# Static column name from delivery_radius_meters
delivery_radius_meters = 'DELIVERY_RADIUS_METERS'
event_started_timestamp = 'EVENT_STARTED_TIMESTAMP'

# Static column name from purchases
time_received = "TIME_RECEIVED"
time_delivered = "TIME_DELIVERED"
delivery_amount = "END_AMOUNT_WITH_VAT_EUR"
distance = "DROPOFF_DISTANCE_STRAIGHT_LINE_METRES"

# Arbitrary naming for columns during the calculation. Could be changed to be adapted to the presentation
duration = 'DURATION'
event_period = 'EVENT_PERIOD'
event_ended_timestamp = 'EVENT_ENDED_TIMESTAMP'

# Set the start and end date for the timeframe provided
start_date = '2020-01-01'
end_date = '2020-12-31'
