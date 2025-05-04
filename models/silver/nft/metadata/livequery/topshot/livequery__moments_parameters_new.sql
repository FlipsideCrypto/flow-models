    {{ config(
        materialized = 'table',
        unique_key = 'contract',
        tags = ['livequery', 'topshot', 'allday', 'moment_metadata']
    ) }}

    SELECT
        'A.0b2a3299cc857e29.TopShot' AS contract,
        'https://public-api.nbatopshot.com/graphql' as base_url,
        'query getMintedMoment($momentId: ID!) {
            getMintedMoment(momentId: $momentId) {
                data {
                id
                version
                sortID
                set {
                    id
                    sortID
                    version
                    flowId
                    flowName
                    flowSeriesNumber
                    flowLocked
                    setVisualId
                    assetPath
                    assets {
                    images {
                        type
                        url
                    }
                    }
                }
                play {
                    id
                    version
                    description
                    flowID
                    sortID
                    status
                    assets {
                    videos {
                        type
                        url
                        videoLength
                    }
                    videoLengthInMilliseconds
                    }
                    stats {
                    playerID
                    playerName
                    firstName
                    lastName
                    jerseyNumber
                    teamAtMoment
                    awayTeamName
                    awayTeamScore
                    homeTeamName
                    homeTeamScore
                    dateOfMoment
                    totalYearsExperience
                    teamAtMomentNbaId
                    height
                    weight
                    currentTeam
                    currentTeamId
                    primaryPosition
                    homeTeamNbaId
                    awayTeamNbaId
                    nbaSeason
                    draftYear
                    draftSelection
                    draftRound
                    birthplace
                    birthdate
                    draftTeam
                    draftTeamNbaId
                    playCategory
                    playType
                    quarter
                    }
                    statsPlayerGameScores {
                    blocks
                    points
                    steals
                    assists
                    minutes
                    rebounds
                    turnovers
                    plusMinus
                    flagrantFouls
                    personalFouls
                    technicalFouls
                    twoPointsMade
                    blockedAttempts
                    fieldGoalsMade
                    freeThrowsMade
                    threePointsMade
                    defensiveRebounds
                    offensiveRebounds
                    pointsOffTurnovers
                    twoPointsAttempted
                    assistTurnoverRatio
                    fieldGoalsAttempted
                    freeThrowsAttempted
                    twoPointsPercentage
                    fieldGoalsPercentage
                    freeThrowsPercentage
                    threePointsAttempted
                    threePointsPercentage
                    playerPosition
                    }
                    statsPlayerSeasonAverageScores {
                    minutes
                    blocks
                    points
                    steals
                    assists
                    rebounds
                    turnovers
                    plusMinus
                    flagrantFouls
                    personalFouls
                    technicalFouls
                    twoPointsMade
                    blockedAttempts
                    fieldGoalsMade
                    freeThrowsMade
                    threePointsMade
                    defensiveRebounds
                    offensiveRebounds
                    pointsOffTurnovers
                    twoPointsAttempted
                    assistTurnoverRatio
                    fieldGoalsAttempted
                    freeThrowsAttempted
                    twoPointsPercentage
                    fieldGoalsPercentage
                    freeThrowsPercentage
                    threePointsAttempted
                    threePointsPercentage
                    efficiency
                    true_shooting_attempts
                    points_in_paint_made
                    points_in_paint_attempted
                    points_in_paint
                    fouls_drawn
                    offensive_fouls
                    fast_break_points
                    fast_break_points_attempted
                    fast_break_points_made
                    second_chance_points
                    second_chance_points_attempted
                    second_chance_points_made
                    }
                    tags {
                    id
                    name
                    title
                    visible
                    hardcourt
                    level
                    }
                }
                flowId
                flowSerialNumber
                price
                forSale
                listingOrderID
                owner {
                    dapperID
                    email
                    flowAddress
                    username
                    profileImageUrl
                    twitterHandle
                    segmentID
                }
                assetPathPrefix
                setPlay {
                    id: ID
                    setID
                    playID
                    flowRetired
                    circulationCount
                    tags {
                    id
                    name
                    title
                    visible
                    hardcourt
                    level
                    }
                }
                createdAt
                acquiredAt
                packListingID
                tags {
                    id
                    name
                    title
                    visible
                    hardcourt
                    level
                }
                }
            }
            }' AS query
