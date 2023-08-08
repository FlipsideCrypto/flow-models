{{ config(
    materialized = 'table'
) }}

SELECT
    'A.0b2a3299cc857e29.TopShot' AS contract,
    'https://public-api.nbatopshot.com/graphql' as base_url,
    'query getMintedMoment ($momentId: ID!) {
            getMintedMoment (momentId: $momentId) {
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
                        ID
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
UNION
SELECT
    'A.e4cf4bdc1751c65d.AllDay' AS contract,
    'https://nflallday.com/consumer/graphql' as base_url,
    'query SearchMomentNFTsV2($input: SearchMomentNFTsInputV2!) {
                    searchMomentNFTsV2(input: $input) {
                        edges {
                        cursor
                        node {
                            id
                            ownerAddress
                            serialNumber
                            flowID
                            distributionFlowID
                            packNFTFlowID
                            editionFlowID
                            owner {
                            id
                            dapperID
                            email
                            phoneNumber
                            username
                            flowAddress
                            profileImageUrl
                            isCurrentTOSSigned
                            }
                            edition {
                            id
                            flowID
                            playFlowID
                            seriesFlowID
                            setFlowID
                            maxMintSize
                            currentMintSize
                            tier
                            description
                            numMomentsOwned
                            numMomentsInPacks
                            numMomentsUnavailable
                            numMomentsBurned
                            series {
                                flowID
                                name
                                active
                            }
                            set {
                                flowID
                                name
                            }
                            play {
                                id
                                flowID
                                metadata {
                                state
                                description
                                league
                                playType
                                videos {
                                    type
                                    url
                                    videoLength
                                }
                                images {
                                    type
                                    url
                                }
                                classification
                                week
                                season
                                playerID
                                playerFullName
                                playerFirstName
                                playerLastName
                                playerPosition
                                playerNumber
                                playerWeight
                                playerHeight
                                playerBirthdate
                                playerBirthplace
                                playerBirthplace
                                playerRookieYear
                                playerDraftTeam
                                playerDraftYear
                                playerDraftRound
                                playerDraftNumber
                                playerCollege
                                teamID
                                gameNflID
                                gameDate
                                homeTeamName
                                homeTeamID
                                homeTeamScore
                                awayTeamName
                                awayTeamID
                                awayTeamScore
                                gameTime
                                gameQuarter
                                gameDown
                                gameDistance
                                teamName
                                }
                            }
                            }
                        }
                        }
                        pageInfo {
                        endCursor
                        hasNextPage
                        }
                        totalCount
                    }
                }' AS query
