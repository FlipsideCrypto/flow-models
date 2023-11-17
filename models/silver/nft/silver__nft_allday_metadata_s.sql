{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    cluster_by = ['_inserted_timestamp::DATE'],
    unique_key = 'nft_id'
) }}

WITH metadata AS (

    SELECT
        *
    FROM
        ref('bronze_api__allday_metadata') }}
    WHERE
        contract = 'A.e4cf4bdc1751c65d.AllDay'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}

qualify ROW_NUMBER() over (
    PARTITION BY id
    ORDER BY
        _inserted_timestamp
) = 1
),
FINAL AS (
    SELECT
        DATA :flowID AS moment_id,
        contract AS nft_collection,
        DATA :id :: STRING AS nflallday_id,
        DATA :serialNumber :: NUMBER AS serial_number,
        DATA: edition :tier :: STRING AS moment_tier,
        DATA :edition :currentMintSize :: NUMBER AS total_circulation,
        DATA :edition :play :metadata :description :: VARCHAR AS moment_description,
        IFF(
            DATA :edition :play :metadata :playerFullName :: STRING = '',
            'N/A',
            DATA :edition :play :metadata :playerFullName :: STRING
        ) AS player,
        DATA :edition :play :metadata: teamName :: STRING AS team,
        DATA :edition :play :metadata :season :: STRING AS season,
        DATA :edition :play :metadata: week :: STRING AS week,
        DATA :edition :play :metadata: classification :: STRING AS classification,
        DATA :edition :play :metadata :playType :: STRING AS play_type,
        DATA :edition :play :metadata :gameDate :: TIMESTAMP AS moment_date,
        DATA :edition :series :name :: STRING AS series,
        DATA :edition: set :name :: STRING AS set_name,
        DATA :edition :play :metadata :videos :: ARRAY AS video_urls,
        DATA :edition :play :: OBJECT AS moment_stats_full,
        _inserted_timestamp
    FROM
        metadata
)
SELECT
    *
FROM
    FINAL



{
  "cursor": "MTczMDAyODUtMjUyYS00NjMwLThkYzAtOWYyYzQ5NDRiM2ZiLDIwMjMtMDItMDNUMjM6MDI6NTcuNDY4MTk2WiwwLA==",
  "node": {
    "distributionFlowID": 178,
    "edition": {
      "currentMintSize": 301,
      "description": "",
      "flowID": 1297,
      "id": "20d7c938-4b74-4e9d-b804-c9c0e07a9057",
      "maxMintSize": 301,
      "numMomentsBurned": 12,
      "numMomentsInPacks": 3,
      "numMomentsOwned": 286,
      "numMomentsUnavailable": 0,
      "play": {
        "flowID": 1297,
        "id": "bd42f51b-4b7d-4c42-9ed4-b08bb2e78768",
        "metadata": {
          "awayTeamID": "5177",
          "awayTeamName": "Seattle Seahawks",
          "awayTeamScore": "32",
          "classification": "PLAYER_MELT",
          "description": "Taysom Hill did a little bit of everything against Seattle. He threw a touchdown pass. He scored three times on the ground, including a 60-yard rumble for a touchdown. He also contributed on special teams when he recovered a fumble that set the Saints up in the red zone. In all, he tallied four total touchdowns, 112 yards rushing, and a fumble recovery in the game, a 39-32 Saints win over Seattle on Oct. 9, 2022.",
          "gameDate": "10/09/2022",
          "gameDistance": "",
          "gameDown": "",
          "gameNflID": "58909",
          "gameQuarter": "",
          "gameTime": "",
          "homeTeamID": "5169",
          "homeTeamName": "New Orleans Saints",
          "homeTeamScore": "39",
          "images": [
            {
              "type": "PLAY_IMAGE_TYPE_CROPPED_ASSET",
              "url": "https://storage.cloud.google.com/dl-nfl-assets-prod/players/NOTaysomHill2022Week5Crop.png"
            }
          ],
          "league": "NFL",
          "playType": "Player Melt",
          "playerBirthdate": "08/23/1990",
          "playerBirthplace": "Pocatello, ID, United States",
          "playerCollege": "Brigham Young",
          "playerDraftNumber": "0",
          "playerDraftRound": "0",
          "playerDraftTeam": "Undrafted",
          "playerDraftYear": "0",
          "playerFirstName": "Taysom",
          "playerFullName": "Taysom Hill",
          "playerHeight": "74",
          "playerID": "00-0033357",
          "playerLastName": "Hill",
          "playerNumber": "7",
          "playerPosition": "TE",
          "playerRookieYear": "2017",
          "playerWeight": "221",
          "season": "2022",
          "state": "PUBLISHED",
          "teamID": "5169",
          "teamName": "New Orleans Saints",
          "videos": [
            {
              "type": "PLAY_VIDEO_TYPE_VERTICAL",
              "url": "https://storage.cloud.google.com/dl-nfl-assets-private/videoContent/2022_W5_SEA_NO_Hill_Taysom_MELT_Vertical_V2_7503.mp4",
              "videoLength": 90830
            },
            {
              "type": "PLAY_VIDEO_TYPE_SQUARE",
              "url": "https://storage.cloud.google.com/dl-nfl-assets-private/videoContent/2022_W5_SEA_NO_Hill_Taysom_MELT_Square_V2_7503.mp4",
              "videoLength": 90830
            }
          ],
          "week": "5"
        }
      },
      "playFlowID": 1297,
      "series": {
        "active": false,
        "flowID": 3,
        "name": "Series 2"
      },
      "seriesFlowID": 3,
      "set": {
        "flowID": 33,
        "name": "Draw it Up"
      },
      "setFlowID": 33,
      "tier": "RARE"
    },
    "editionFlowID": 1297,
    "flowID": "4151993",
    "id": "17300285-252a-4630-8dc0-9f2c4944b3fb",
    "owner": {
      "dapperID": "google-oauth2|108953105863472483420",
      "email": null,
      "flowAddress": "21f811ffa6cadb01",
      "id": "e1f9fb39-c7b2-4167-9cbf-44d6b4472ade",
      "isCurrentTOSSigned": true,
      "phoneNumber": null,
      "profileImageUrl": "https://storage.googleapis.com/dapper-profile-icons/avatar-nba-nets.png",
      "username": "meka"
    },
    "ownerAddress": "21f811ffa6cadb01",
    "packNFTFlowID": "1285675",
    "serialNumber": 19
  }
}