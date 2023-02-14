{{ config (
    materialized = 'view',
    tags = ['nft', 'dapper'],
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'NFT, ALLDAY, GOLAZOS, TOPSHOT' }}}
) }}

SELECT
    event_contract AS nft_collection,
    nft_id,
    serial_number,
    max_mint_size,
    play_id,
    series_id,
    series_name,
    set_id,
    set_name,
    edition_id,
    tier,
    metadata
FROM
    {{ ref('silver__nft_moment_metadata_final') }}
WHERE
    tx_id NOT IN (
        'bc72100e69b2190e7d65fc1cbf2abd405868edd65a6e8d9d3df1dfc2ee96d02f',
        '76c47a7ea2a4ee89bed4bf9b5dedd4a7d46538c65658d21d50fecd0d772e43c4',
        '31687fe4e09d90a7bdf8339df90bb80f88b359dff504a465434edd273c4e8e2f',
        '53bc1bacdb94a555a94d07d17d5031e6ad96d44cc235bd00443e09aaa20d01de',
        '826a61da11cf10143719fd542f4aecf0a3b97ca0fa1c3aa0e8b29f8e6fbd1d4d',
        '0f3ccb9985cdea03a8030d6385976c11621860368d4cd708d62e5310f41401de',
        'a0bc00d924314d77324c4462d62eecb8cc655077f3eea9238a44381fafcd3ae0',
        '7967530d822124f0ec2a189028b420e772f0983a8c9cdee375f22bcf645928ea',
        '33a17eb94f9487d61a94dcb3d7568f55eefc08af9e054eb80dafd62508a2ba3c',
        '5f83d523a6a27bb4e3b468065101e90c8aa910f8144e914b7820cc7be4d3f3cb',
        '3c43c1e1b7bcc94fbbc9632534f8a4f69b03ddae06f44bcb1d44fabede9c57a4',
        'e1f75e5fdd76852d34b8ba68449e18a13b0b6cd1704eb4677ed1481b5ee40e69',
        'ce60b06f41e7401146c2ddcf25b1f97fc7dfc2a44d647a02f2f687bf18f7c73d',
        '4333cea52be6e0f5a268ca1c48ca914e8ff20b6edf4f660c2cad43403ddd1aad',
        'b4cf4815a09b30ff3a489834767285b2390ca37b632fccb6b24e8ec85821d610',
        '27074fe2b42e07becd40d2f589de64731899221fa717232fb6cd9917bafdbf60'
    )
