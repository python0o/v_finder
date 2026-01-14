# Accepted column aliases in acs_county.csv:
# Required: GEOID (5-digit county FIPS)
#
# Population:
POP_ALIASES = ("population", "POP", "TotalPop", "B01001_001E")
#
# Ethnicity/race: provide either minority_share directly or components
MINORITY_SHARE_ALIASES = ("minority_share", "MinorityShare", "pct_minority")
# If you provide white_non_hisp_share, minority_share = 1 - that
WHITE_NH_SHARE_ALIASES = ("white_non_hisp_share", "WhiteNHShare", "pct_white_non_hisp")

NAME_ALIASES = ("NAME", "Name", "county_name")