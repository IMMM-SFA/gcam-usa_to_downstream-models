# maps GCAM-USA grid regions to individual states
REGION_TO_STATES = {
    "Alaska grid": ["AK"],
    "California grid": ["CA"],
    "Central East grid": ["IN", "KY", "MI", "OH", "WV"],
    "Central Northeast grid": ["IL", "MO", "WI"],
    "Central Northwest grid": ["IA", "MN", "ND", "NE", "SD"],
    "Central Southwest grid": ["KS", "OK"],
    "Florida grid": ["FL"],
    "Hawaii grid": ["HI"],
    "Mid-Atlantic grid": ["DC", "DE", "MD", "NJ", "PA"],
    "New England grid": ["CT", "MA", "ME", "NH", "RI", "VT"],
    "New York grid": ["NY"],
    "Northwest grid": ["ID", "MT", "NV", "OR", "UT", "WA"],
    "Southeast grid": ["AL", "AR", "GA", "LA", "MS", "NC", "SC", "TN", "VA"],
    "Southwest grid": ["AZ", "CO", "NM", "WY"],
    "Texas grid": ["TX"],
}

# map USA to individual states
COUNTRY_TO_STATES = {
    "USA": ["AK", "CA", "IN", "KY", "MI", "OH", "WV", "IL", "MO", "WI","IA", "MN", "ND", 
            "NE", "SD", "KS", "OK", "FL", "HI", "DC", "DE", "MD", "NJ", "PA", "CT", "MA", 
            "ME", "NH", "RI", "VT", "NY", "ID", "MT", "NV", "OR", "UT", "WA", "AL", "AR", 
            "GA", "LA", "MS", "NC", "SC", "TN", "VA", "AZ", "CO", "NM", "WY", "TX"]}