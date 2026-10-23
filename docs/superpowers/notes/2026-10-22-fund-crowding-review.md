# Fund-crowding precision review (seed 1)

87 funds have 2+ approved holders; 50 sampled.

## Verdict (Claude, 2026-10-22, after the review fixes)

**Identity: 0 false merges in 50 funds (about 120 holder rows).** Every row
in each fund below names the same fund. Parallel vehicles rolled up as
intended (Hg Saturn 4 A, Bridgepoint Europe VIII B, SVP "Feeder" VI), and
Francisco Partners VIII and Agility IV stayed separate.

One to check:
- **Clearlake Capital Partners VII:** NHRS's row reads "Clearlake Capital
  Group Fund VII (direct lending fund)". Clearlake's only fund VII is the
  flagship PE fund; its credit funds are "Opportunities Partners" I–III. So
  this is very likely the same fund with the summariser's strategy label
  wrong, but a person should confirm.

Not identity problems, but worth knowing:
- **Status labels.** A few rows counted as approved read like
  consideration rather than a vote: Nebraska / Realterm V ("under review"),
  Ohio P&F / Chicago Pacific Founders IV ("consideration of commitment"),
  and Maine / GC Creation III ("board considered"). About 3 of ~120 rows.
  Tightening the Haiku prompt and re-extracting would cost about $1.40.
- **Amounts.** The amount shown is the latest approved figure found, which
  can come from a different document than the quote. Examples: SDCERS PRISA
  III shows $7.2M against a "$50 Million" quote, and Rhode Island ECP VI
  shows $2M against "$30 million". Treat amounts as indicative.

## PRISA III Fund
`pgim|prisa|3`

- **San Diego City Employees' Retirement System**: "PRISA III Fund", $7.2M, 2025-05-08, exact. Approve $50 Million Commitment to PRISA III Fund for the Real Estate Portfolio
- **North Dakota State Investment Board**: "PRISA III", $85M, 2026-03-13, exact. PRISA III open-end US value-add real estate fund commitment fully invested

## Manulife Comvest Credit Opportunities Fund II
`manulife comvest|credit opportunities|2`

- **Illinois Municipal Retirement Fund**: "Manulife Comvest Credit Opportunities Fund II, L.P.", $75M, 2026-05-29, exact. Commitment to Manulife Comvest Credit Opportunities Fund II, subject to satisfactory legal due diligence
- **New York State Teachers' Retirement System**: "Manulife Comvest Credit Opportunities Fund II", $200M, 2026-04-29, exact. New commitment to Manulife Comvest Credit Opportunities Fund II

## Kingswood Capital Opportunities Fund IV
`kingswood capital|opportunities|4`

- **Arkansas Teacher Retirement System**: "Kingswood Capital Opportunities IV, L.P.", $50M, 2026-06-01, exact. (Prior meeting, April 2026) Investment in Kingswood Capital Opportunities IV, L.P. with Imminent Need
- **Los Angeles County Employees Retirement Association**: "Kingswood Capital Opportunities Fund IV", $220M, 2026-08-12, exact. Committed $220 million to Kingswood Capital Opportunities Fund IV, a middle-market buyout fund focused on industrials, healthcare, consumer staples, and consumer discretionary in North America

## Ares Real Estate Secondaries Fund X, L.P.
`ares|real estate secondaries|10`

- **Arkansas Teacher Retirement System**: "Ares Real Estate Secondaries Fund X, L.P.", $75M, 2026-09-28, exact. Investment in Ares Real Estate Secondaries Fund X, L.P., a continuation of the Landmark secondaries fund series targeting 13-16% net IRR
- **Nebraska Investment Council**: "Ares Real Estate Secondaries X", $50M, 2026-06-11, exact. Committed $50 million to Ares Real Estate Secondaries X with allocations across multiple plans

## New Enterprise Associates 19, L.P.
`new enterprise associates||19`

- **Illinois Municipal Retirement Fund**: "New Enterprise Associates 19, L.P.", n/a, 2026-03-27, exact. Commitment to New Enterprise Associates 19, L.P. and NEA 19 Venture Growth Equity, L.P., subject to satisfactory due diligence
- **Los Angeles City Employees' Retirement System**: "New Enterprise Associates 19, L.P.", $40M, 2026-06-23, exact. Commitment to New Enterprise Associates 19, L.P.
- **Nebraska Investment Council**: "New Enterprise Associates Fund 19", n/a, undated, exact. Approved commitment to New Enterprise Associates Fund 19 as part of annual pacing study for private markets

## Pantheon Global Infrastructure Fund V
`pantheon|global infrastructure|5`

- **Arkansas Public Employees Retirement System**: "Pantheon Global Infrastructure V", $100M, 2025-06-11, exact. Approved secondary infrastructure commitment to Pantheon Global Infrastructure V
- **Cook County Pension Fund**: "Pantheon Global Infrastructure Fund V", $150M, 2026-06-04, exact. Follow-on investment in Pantheon Global Infrastructure Fund V

## ICG Europe IX
`icg intermediate capital|europe|9`

- **New Hampshire Retirement System**: "ICG Europe Fund IX", $100M, 2025-10-14, exact. Commit up to $100 million to ICG Europe Fund IX, subject to contract and legal review
- **Virginia Retirement System**: "ICG Europe IX", $330M, 2025-10-10, exact. Hired ICG Europe IX, a closed-end fund targeting subordinated debt and equity investments in European mid-market companies

## Apax Digital Fund III
`apax partners|digital|3`

- **Louisiana State Employees' Retirement System**: "Apax Digital Fund III", $100M, 2026-05-01, exact. Commit $100 million to Apax Digital Fund III
- **Virginia Retirement System**: "Apax Digital Fund III", $201M, 2026-09-17, exact. Committed $201 million to Apax Digital Fund III, a middle market tech-focused private equity strategy

## Clearlake Capital Partners VII
`clearlake capital partners||7`

- **Alaska Retirement Management Board**: "Clearlake Capital Partners VII", $50M, 2021-12-02, exact. CIO delegated $50 million private equity commitment to Clearlake Capital Partners VII (middle market buyout/special situations in technology, industrials, consumer)
- **Minnesota State Board of Investment**: "Clearlake Capital Partners VII", $100M, 2022-02-24, exact. Clearlake Capital Partners VII
- **New Hampshire Retirement System**: "Clearlake Capital Group Fund VII", $50M, 2022-03-08, exact. Committed $50 million to Clearlake Capital Group Fund VII (direct lending fund)

## Brookfield Strategic Real Estate Partners V
`brookfield asset|strategic real estate partners|5`

- **Minnesota State Board of Investment**: "Brookfield Strategic Real Estate Partners V", $200M, 2023-08-23, exact. Authorize Executive Director to negotiate and execute commitment to Brookfield Strategic Real Estate Partners V
- **New York City Retirement Systems (Comptroller)**: "Brookfield Strategic Real Estate Partners V", $50M, undated, exact. New private real estate commitments including AEW Partners X ($30M), Brookfield Strategic Real Estate Partners V ($50M), Carlyle Realty Partners X ($30M), LBA Logistics Value Fund X ($50M), and KKR Real Estate Partners Americas IV ($40M)

## Monarch Capital Partners VII, L.P.
`monarch alternative capital||7`

- **Seattle City Employees' Retirement System**: "Monarch Capital Partners VII LP", $30M, 2026-05-01, exact. Commitment of up to $30 million to Monarch Capital Partners VII LP, an opportunistic credit strategy (second allocation to Monarch)
- **Washington State Investment Board**: "Monarch Capital Partners VII, L.P.", $300M, 2026-04-16, exact. Follow-on commitment to Monarch Capital Partners VII, L.P., a distressed/opportunistic private credit fund, supporting the WSIB's standalone private credit allocation

## Blue Owl Real Estate Fund VII, L.P.
`blue owl capital|real estate|7`

- **Los Angeles City Employees' Retirement System**: "Blue Owl Real Estate VII L.P.", $50M, 2026-10-13, exact. Commitment of up to $50 million in Blue Owl Real Estate VII L.P. (referenced from Sept 8, 2026 meeting minutes)
- **New York State Common Retirement Fund**: "Blue Owl Real Estate Fund VII, L.P.", $300M, 2026-03-01, exact. Committed to Blue Owl Real Estate Fund VII, L.P. focused on net-leased real estate with investment grade tenants

## Spark Capital Growth VI, L.P.
`spark capital|growth|6`

- **Delaware Public Employees' Retirement System**: "Spark Capital Growth Fund VI", n/a, 2026-03-27, exact. Committed up to $40 million to Spark Capital Fund IX and Spark Capital Growth Fund VI
- **Los Angeles City Employees' Retirement System**: "Spark Capital Growth Fund VI, L.P.", $35M, 2026-06-23, exact. Commitment to Spark Capital Growth Fund VI, L.P.
- **Washington State Investment Board**: "Spark Capital Growth VI, L.P.", n/a, 2026-04-16, exact. Commitment to Spark Capital IX, L.P. (early-stage venture) and Spark Capital Growth VI, L.P. (growth-stage venture), focused on North American venture investments including AI

## Oaktree Special Situations Fund IV
`oaktree capital|special situations|4`

- **San Joaquin County Employees' Retirement Association**: "Oaktree Special Situations Fund IV", $50M, 2026-02-13, exact. Committed $50 million to Oaktree Special Situations Fund IV per Resolution 2026-02-01
- **Teachers' Retirement System of Louisiana**: "Oaktree Special Situations Fund IV, L.P.", $150M, 2025-08-25, exact. Approved commitment to Oaktree Special Situations Fund IV, L.P. subject to final term negotiations

## Lead Edge Capital VII, LP
`lead edge capital||7`

- **Michigan Public School Employees' Retirement System**: "Lead Edge Capital VII, LP", $100M, 2026-03-25, exact. Lead Edge Capital VII, LP commitment
- **Virginia Retirement System**: "Lead Edge Capital Fund VII", $150M, 2025-12-05, exact. Hired Lead Edge Capital Fund VII, a growth equity firm investing in software, internet, and technology-enabled services companies

## Francisco Partners Fund VIII
`francisco partners||8`

- **Illinois Municipal Retirement Fund**: "Francisco Partners VIII, L.P.", n/a, 2026-03-27, exact. Approve allocation to Francisco Partners VIII, L.P.
- **Los Angeles City Employees' Retirement System**: "Francisco Partners VIII, L.P.", $50M, 2026-06-23, exact. Commitment to Francisco Partners VIII, L.P.
- **Louisiana State Employees' Retirement System**: "Francisco Partners Fund VIII", $100M, 2026-05-01, exact. Committed $100 million to Francisco Partners Fund VIII
- **Nebraska Investment Council**: "Francisco Partners Fund VIII", $30M, 2026-02-26, exact. Committed $30 million to Francisco Partners Fund VIII; $26M to DB/CBB Plans, $2M to OSERS, $1M to General Endowments, $1M to Health Care Endowment; subject to legal review
- **New York State Common Retirement Fund**: "Francisco Partners Fund VIII", $400M, 2026-05-01, exact. Committed $400 million to Francisco Partners Fund VIII targeting technology sector investments in North America
- **School Employees Retirement System of Ohio**: "Francisco Partners VIII", $125M, 2026-10-15, exact. Commitment to Francisco Partners VIII technology-focused buyout fund
- **Pennsylvania State Employees' Retirement System**: "Francisco Partners VIII, L.P.", $100M, 2026-02-24, exact. Commitment to Francisco Partners VIII, L.P. as follow-on investment within Private Equity asset class, subject to successful completion of contract negotiations and Commonwealth legal approvals within 12 months

## Turning Rock Fund IV
`turning rock||4`

- **Illinois Municipal Retirement Fund**: "Turning Rock Fund IV, L.P.", n/a, 2026-05-29, exact. Aggregate commitment to Turning Rock Fund IV and Turning Rock Fund IV Co-Investment Fund, subject to satisfactory legal due diligence
- **Philadelphia Board of Pensions and Retirement**: "Turning Rock Fund IV", $25M, 2026-08-27, exact. Private credit commitment to Turning Rock Fund IV

## JFL Equity Investors VII, L.P.
`jfl|equity investors|7`

- **Arkansas Teacher Retirement System**: "JFL Equity Investors VII, L.P.", $50M, 2026-06-01, exact. (Prior meeting, April 2026) Investment in JFL Equity Investors VII, L.P. with Imminent Need
- **New York State Teachers' Retirement System**: "JFL Equity Investors VII", $200M, 2026-04-29, exact. New commitment to JFL Equity Investors VII

## Brookfield Super-Core Infrastructure Partners
`brookfield asset|super-core infrastructure partners|`

- **Alameda County Employees' Retirement Association**: "Brookfield Super-Core Infrastructure Partners", $40M, undated, unnumbered. Adopted a $40 million investment in Brookfield Super-Core Infrastructure Partners as part of ACERA's Real Assets Portfolio – Infrastructure
- **Kansas Public Employees Retirement System**: "Brookfield Super-Core Infrastructure Partners L.P.", $140M, 2025-05-16, unnumbered. Additional commitment of up to $140 million to Brookfield Super-Core Infrastructure Partners L.P.

## Ares Pathfinder Fund III
`ares|pathfinder|3`

- **DC Retirement Board**: "Ares Pathfinder Fund III", $100M, 2026-07-16, exact. Closed $100 million commitment with Ares Pathfinder Fund III
- **Seattle City Employees' Retirement System**: "Ares Pathfinder Fund III", $30M, 2026-05-01, exact. Commitment of up to $30 million to Ares Pathfinder Fund III, an asset-based private credit strategy investing in loans, leases, receivables, and royalties
- **San Mateo County Employees' Retirement Association**: "Ares Pathfinder III", $20M, 2026-06-02, exact. Commitment to Ares Pathfinder III, an asset-backed private credit fund, made under delegation of authority
- **Virginia Retirement System**: "Ares Pathfinder III", $400M, 2026-06-17, exact. Hired Ares Pathfinder III, a closed-end fund targeting directly originated asset-backed structured investments

## Stellex Capital Partners III, L.P.
`stellex capital|partners|3`

- **Kansas Public Employees Retirement System**: "Stellex Capital Partners III LP", $90M, 2024-09-13, exact. Commitment to Stellex Capital Partners III LP for private equity investment pending legal review
- **New Jersey Division of Pensions and Benefits**: "Stellex Capital Partners III, L.P.", $125M, 2025-04-30, exact. Commitment of up to $125 million in Stellex Capital Partners III, L.P. for middle market buyout investments in North America and Europe

## Realterm Logistics Fund V
`realterm|logistics|5`

- **Kansas Public Employees Retirement System**: "Realterm Logistics Fund V, L.P.", $100M, 2026-05-22, exact. Commitment of up to $100 million to Realterm Logistics Fund V, L.P. pending legal review
- **Nebraska Investment Council**: "Realterm Logistics Fund V", $40M, 2026-06-11, exact. Realterm Logistics Fund V - Logistics and real assets commitment under review

## Spark Capital IX, L.P.
`spark capital||9`

- **Delaware Public Employees' Retirement System**: "Spark Capital Fund IX", n/a, 2026-03-27, exact. Committed up to $40 million to Spark Capital Fund IX and Spark Capital Growth Fund VI
- **Los Angeles City Employees' Retirement System**: "Spark Capital IX, L.P.", $15M, 2026-06-23, exact. Commitment to Spark Capital IX, L.P.
- **Washington State Investment Board**: "Spark Capital IX, L.P.", n/a, 2026-04-16, exact. Commitment to Spark Capital IX, L.P. (early-stage venture) and Spark Capital Growth VI, L.P. (growth-stage venture), focused on North American venture investments including AI

## Ares US Real Estate Fund XI, L.P.
`ares|us real estate|11`

- **New York City Retirement Systems (Comptroller)**: "Ares US Real Estate Fund XI, L.P.", $150M, 2026-06-04, exact. New real estate commitments including Ares Industrial Real Estate Fund, Ares US Real Estate Fund XI, PCCP Equity X, and Raith Real Estate IV
- **North Dakota State Investment Board**: "Ares US Real Estate Fund XI, L.P.", $75M, 2026-02-13, exact. New commitment to Ares US Real Estate Fund XI, L.P., a value-add real estate fund ($35M Pension Pool, $40M Legacy Fund)

## Bain Capital Fund XIV, L.P.
`bain capital||14`

- **Illinois Firefighters' Pension Investment Fund**: "Bain Capital Fund XIV", n/a, 2025-04-25, exact. Selected and approved Bain Capital Fund XIV for private equity large market buyout investment
- **New Jersey Division of Pensions and Benefits**: "Bain Capital Fund XIV, L.P.", $200M, 2025-01-29, exact. Approved commitment of up to $200 million in Bain Capital Fund XIV, L.P., a North American-focused buyout fund targeting consumer and retail, financial & business services, industrials, TMT, and healthcare sectors

## StepStone VC Secondaries Fund VII
`stepstone|vc secondaries|7`

- **Metropolitan Government of Nashville Employees' Benefit Trust Fund**: "StepStone VC Secondaries Fund VII", $10M, 2026-04-24, exact. Commit to StepStone VC Secondaries Fund VII, a fund of funds investing in secondary markets with allocation to direct secondaries (~75%), LP Interests (~12.5%), and GP-led secondaries (~12.5%)
- **School Employees Retirement System of Ohio**: "StepStone VC Secondaries Fund VII", $50M, 2026-10-15, exact. Commitment to Stepstone VC Secondaries VII, a venture capital secondaries strategy

## Raith Real Estate IV
`raith capital partners|real estate|4`

- **New York City Retirement Systems (Comptroller)**: "Raith Real Estate IV", $70M, 2026-06-04, exact. New real estate commitments including Ares Industrial Real Estate Fund, Ares US Real Estate Fund XI, PCCP Equity X, and Raith Real Estate IV
- **New York State Teachers' Retirement System**: "Raith Real Estate IV", $75M, 2026-04-29, exact. Closed commitment to Raith Real Estate IV

## Accel Leaders 5, LP
`accel|leaders|5`

- **Delaware Public Employees' Retirement System**: "Accel Leaders Fund 5", $60M, 2025-10-31, exact. Commitment of up to $60 million to Accel Leaders Fund 5, subject to legal review
- **Michigan Public School Employees' Retirement System**: "Accel Leaders 5, LP", $35M, 2026-03-25, exact. Accel Leaders 5, LP commitment

## KKR Global Infrastructure Investors V
`kkr|global infrastructure investors|5`

- **Alaska Retirement Management Board**: "KKR Global Infrastructure Investors V", $75M, 2026-09-15, exact. Made $75 million commitment to KKR Global Infrastructure Investors V, a closed-end fund focused on energy, power, transport, industrial, digital, waste, and water in OECD countries
- **Maine Public Employees Retirement System**: "KKR Global Infrastructure Investors V", $25M, 2026-05-14, exact. Commitment to KKR Global Infrastructure Investors V
- **Minnesota State Board of Investment**: "KKR Global Infrastructure Investors V", $200M, 2024-05-21, exact. Authorized commitment to KKR Global Infrastructure Investors V up to $200 million
- **New York City Retirement Systems (Comptroller)**: "KKR Global Infrastructure Investors V", $258M, undated, exact. New infrastructure commitments including KKR Global Infrastructure Investors V, InfraVia European Fund VI, Manulife Infrastructure Fund III, Asterion Industrial Infra Fund III, ASF IX Infrastructure B, Actis Energy 6, and ICG Infrastructure Fund II
- **New York State Teachers' Retirement System**: "KKR Global Infrastructure Investors V", $100M, 2026-04-29, exact. New commitment to KKR Global Infrastructure Investors V

## Hg Saturn 4 A
`hg|saturn|4`

- **New Jersey Division of Pensions and Benefits**: "Hg Saturn 4 Fund, L.P.", $150M, 2025-01-29, exact. Approved commitment of up to $150 million in Hg Saturn 4 Fund, L.P., a specialty software and tech-services investment fund targeting European-focused buyouts
- **New York City Retirement Systems (Comptroller)**: "Hg Saturn 4 A", $60.6M, 2026-03-05, exact. New commitment to Hg Saturn 4 A and related co-investment
- **New York State Teachers' Retirement System**: "Hg Saturn 4 A LP", $150M, 2025-04-23, exact. Private equity/debt new commitments approved via ED/CIO discretion: Blue Torch Credit Opp Fund IV (renew $300M), Carlyle Asia Partners VI & Bluebird (renew $200M), MBK Partners Fund VI & Oak Co-Invest (renew $200M), Integrum Capital Partners II & Co-Invest (new $200M), JLL Partners Fund IX (new $200

## Stonepeak Infrastructure Fund V, L.P.
`stonepeak|infrastructure|5`

- **Illinois Firefighters' Pension Investment Fund**: "Stonepeak Infrastructure Fund V", $40M, 2024-12-20, exact. Approved $40 million allocation to Stonepeak Infrastructure Fund V
- **New York City Retirement Systems (Comptroller)**: "Stonepeak Infrastructure Fund V, L.P.", $26.4M, undated, exact. New capital commitment to Stonepeak Infrastructure Fund V, L.P.

## Warburg Pincus Global Growth 14
`warburg pincus|global growth|14`

- **Minnesota State Board of Investment**: "Warburg Pincus Global Growth 14", $300M, 2021-12-01, exact. Commitment of up to $300 million or 20% of Warburg Pincus Global Growth 14, whichever is less
- **New Hampshire Retirement System**: "Warburg Pincus Global Growth Fund 14", $50M, 2022-04-12, exact. Commit $50 million to Warburg Pincus Global Growth Fund 14, subject to contract and legal review

## Bridgepoint Europe VIII
`bridgepoint|europe|8`

- **Minnesota State Board of Investment**: "Bridgepoint Europe VIII", $200M, 2026-03-26, exact. Authorized commitment to Bridgepoint Europe VIII
- **New York City Retirement Systems (Comptroller)**: "Bridgepoint Europe VIII B", n/a, undated, exact. Numerous new private equity fund commitments and co-investments initiated in 2025-2026 vintage including KKR North America Fund XIV, Thoma Bravo Partners XVI, Hg Genesis 11, Green Equity Investors X, Bridgepoint Europe VIII B, Lexington Capital Partners XI, and others
- **New York State Common Retirement Fund**: "Bridgepoint Europe VIII", $376M, 2026-06-01, exact. New commitment to Bridgepoint Europe VIII for healthcare, advanced industrials, services, and technology sector investments in Western Europe
- **Public Employee Retirement System of Idaho**: "Bridgepoint Europe VIII, L.P.", $82.7M, undated, exact. Committed to Bridgepoint Europe VIII, L.P.
- **School Employees Retirement System of Ohio**: "Bridgepoint Europe VIII", $50M, 2026-10-15, exact. New fund commitment to Bridgepoint Europe VIII plus sidecar co-investment

## Charterhouse Capital Partners XII, L.P.
`charterhouse capital partners||12`

- **Virginia Retirement System**: "Charterhouse XII", $215M, 2026-06-17, exact. Hired Charterhouse XII, a middle market PE firm focused on European services and healthcare companies
- **Washington State Investment Board**: "Charterhouse Capital Partners XII, L.P.", $150M, 2026-04-16, exact. Commitment to Charterhouse Capital Partners XII, L.P., a European mid-market private equity fund focused on services and healthcare sectors in Western Europe

## Blackstone Infrastructure Partners, L.P.
`blackstone|infrastructure partners|`

- **Arkansas Teacher Retirement System**: "Blackstone Infrastructure Partners, L.P.", $75M, 2026-09-28, unnumbered. Investment in Blackstone Infrastructure Partners, L.P., targeting 10% IRR with 4-5% cash yield in energy, digital, transportation, and utilities
- **New York State Teachers' Retirement System**: "Blackstone Infrastructure Partners", $150M, 2026-04-29, unnumbered. New commitment to Blackstone Infrastructure Partners

## Francisco Partners Agility IV, L.P.
`francisco partners|agility|4`

- **Illinois Municipal Retirement Fund**: "Francisco Partners Agility IV, L.P.", n/a, 2026-03-27, exact. Commitment to Francisco Partners VIII, L.P. and Francisco Partners Agility IV, L.P., subject to satisfactory due diligence
- **Los Angeles City Employees' Retirement System**: "Francisco Partners Agility IV, L.P.", $5M, 2026-10-13, exact. Commitment to Francisco Partners Agility IV, L.P.
- **Nebraska Investment Council**: "Francisco Partners Agility IV", $10M, 2026-02-26, exact. Committed $10 million to Francisco Partners Agility IV; $9M to DB/CBB Plans, $1M to OSERS; subject to legal review
- **New York State Common Retirement Fund**: "Francisco Partners Agility IV", $150M, 2026-05-01, exact. Committed $150 million to Francisco Partners Agility IV for technology sector investments in North America
- **School Employees Retirement System of Ohio**: "Francisco Partners Agility IV", $60M, 2026-10-15, exact. Commitment to Francisco Partners Agility IV
- **Pennsylvania State Employees' Retirement System**: "Francisco Partners Agility IV, L.P.", $30M, 2026-02-24, exact. Commitment to Francisco Partners Agility IV, L.P. as follow-on investment within Private Equity asset class, subject to successful completion of contract negotiations and Commonwealth legal approvals within 12 months

## Chicago Pacific Founders Healthcare Real Estate Fund IV, L.P.
`chicago pacific founders|healthcare real estate|4`

- **Illinois Municipal Retirement Fund**: "Chicago Pacific Founders Healthcare Real Estate Fund IV, L.P.", n/a, 2026-08-27, exact. Approve allocation to Chicago Pacific Founders Healthcare Real Estate Fund IV, L.P.
- **Ohio Police & Fire Pension Fund**: "Chicago Pacific Founders Healthcare Real Estate Fund IV, L.P.", $50M, 2026-08-26, exact. Consideration of commitment to Chicago Pacific Founders Healthcare Real Estate Fund IV

## Ares Industrial Real Estate Fund
`ares|industrial real estate|`

- **Arkansas Teacher Retirement System**: "Ares Industrial Real Estate Fund L.P.", $50M, 2026-02-02, unnumbered. Additional investment of up to $50 million in Ares Industrial Real Estate Fund L.P.
- **New York City Retirement Systems (Comptroller)**: "Ares Industrial Real Estate Fund", $93M, 2026-03-18, unnumbered. New commitment to Ares Industrial Real Estate Fund LP

## GC Creation Fund III
`general catalyst|gc creation|3`

- **Los Angeles City Employees' Retirement System**: "GC Creation Fund III, L.P.", $15M, 2026-10-13, exact. Commitment of up to $15 million in GC Creation Fund III, L.P., an early-stage venture capital fund
- **Maine Public Employees Retirement System**: "GC Creation Fund III", $15M, 2026-06-11, exact. Board considered commitment to General Catalyst Group XIII and GC Creation Fund III (discussed in executive session per Board Policy)
- **New York State Common Retirement Fund**: "GC Creation Fund III", $40M, 2026-07-01, exact. New commitment to GC Creation Fund III for multi-stage venture investments in technology companies

## LS Power Fund VI
`ls power||6`

- **San Mateo County Employees' Retirement Association**: "LS Power Fund VI", $25M, 2026-07-28, exact. SamCERA utilized CEO delegated authority to execute a re-up commitment to LS Power Fund VI (reported at June 2026 meeting, referencing prior CIO report)
- **Virginia Retirement System**: "LS Power Fund VI", $200M, 2026-06-17, exact. Hired LS Power Fund VI, a closed-end fund investing in power generation and energy transition infrastructure

## ISQ Global Infrastructure Fund IV (UST), LP
`i squared capital|global infrastructure # ust|4`

- **Arkansas Teacher Retirement System**: "ISQ Global Infrastructure Fund IV (UST), LP", $75M, 2026-02-02, exact. ISQ Global Infrastructure Fund IV (UST), LP closed - infrastructure fund focused on utilities, transportation, communication, energy, and social infrastructure
- **New York City Retirement Systems (Comptroller)**: "ISQ Global Infrastructure Fund IV (UST), L.P.", $220M, 2026-03-05, exact. New commitment to ISQ Global Infrastructure Fund IV (UST)

## Summit Partners Growth Equity Fund XII
`summit partners|growth equity|12`

- **Delaware Public Employees' Retirement System**: "Summit Partners Growth Equity Fund XII", $80M, 2024-07-26, exact. Commitment of up to $120 million to Summit Partners Growth Equity Fund XII with $80 million to Summit GE XII and $40 million to potential co-investments, subject to legal review
- **Minnesota State Board of Investment**: "Summit Partners Growth Equity Fund XII", $250M, 2024-08-14, exact. Authorized commitment to Summit Partners Growth Equity Fund XII

## OceanSound Partners Fund III
`oceansound partners||3`

- **Nebraska Investment Council**: "OceanSound Partners Fund III", $40M, 2026-02-26, exact. Committed $40 million to OceanSound Partners Fund III; $34M to DB/CBB Plans, $3M to OSERS, $2M to General Endowments, $1M to Health Care Endowment; subject to legal review
- **New York City Retirement Systems (Comptroller)**: "OceanSound Partners Fund III", $11.1M, 2026-09-23, exact. New private equity commitments closed subsequent to 12/31/2025 including OceanSound Partners Fund III, L.P. and related co-invest sidecar.

## One Rock Capital Partners IV, L.P.
`one rock capital partners||4`

- **DC Retirement Board**: "One Rock Capital Partners IV, L.P.", $75M, 2026-03-31, exact. Committed to One Rock Capital Partners IV, L.P. buyout fund
- **Nebraska Investment Council**: "OneRock Capital Partners Fund IV", n/a, 2020-12-31, exact. Approved commitment to OneRock Capital Partners Fund IV

## Strategic Value Special Situations Feeder Fund VI, L.P.
`strategic value partners|special situations|6`

- **Kentucky Public Pensions Authority**: "Strategic Value Partners Special Situations Fund VI", n/a, 2026-02-25, exact. Recent private equity commitment to Strategic Value Partners Special Situations Fund VI (September 2025)
- **New Jersey Division of Pensions and Benefits**: "Strategic Value Special Situations Feeder Fund VI, L.P.", $100M, 2025-01-29, exact. Approved commitment of up to $100 million in Strategic Value Special Situations Feeder Fund VI, L.P., focused on distressed market opportunities

## Demopolis Equity Partners Fund I L.P.
`demopolis equity partners||1`

- **New York State Common Retirement Fund**: "Demopolis Equity Partners Fund I L.P.", $16.76M, 2026-02-01, exact. Committed to Demopolis Equity Partners Fund I L.P. through M2 NY Pioneer Fund III for lower middle market technology-enabled business investments
- **School Employees Retirement System of Ohio**: "Demopolis I", $30M, 2026-10-15, exact. First-time investment commitment to Demopolis I, a lower middle market growth equity fund

## Energy Capital Partners VI, LP
`energy capital partners||6`

- **Los Angeles County Employees Retirement Association**: "Energy Capital Partners VI, LP", $200M, 2026-10-14, exact. Commitment to Energy Capital Partners VI, LP with focus on energy transition, electricity infrastructure and sustainability infrastructure in North America and Europe
- **Minnesota State Board of Investment**: "ECP VI, L.P.", $250M, 2026-03-26, exact. Authorized commitment to ECP VI, L.P.
- **Employees' Retirement System of Rhode Island**: "Energy Capital Partners VI", $2M, 2026-05-20, exact. Rhode Island Employees' Retirement Systems Pooled Trust committed up to $30 million to Energy Capital Partners VI, a private real assets/infrastructure fund

## Quantum Energy Partners IX
`quantum energy partners||9`

- **DC Retirement Board**: "Quantum Energy Partners IX", $100M, 2026-03-31, exact. Closed on a $100 million re-up commitment to Quantum Energy Partners IX, a private natural resources fund, on November 26, 2025
- **Teachers' Retirement System of Louisiana**: "Quantum Energy Partners IX, L.P.", $75M, 2026-07-10, exact. Approved commitment of up to $75 million to Quantum Energy Partners IX, L.P., subject to final term negotiations

## Tiger Infrastructure Partners Fund IV
`tiger infrastructure partners||4`

- **DC Retirement Board**: "Tiger Infrastructure Partners Fund IV", $100M, 2026-03-31, exact. Committed to Tiger Infrastructure Partners Fund IV opportunistic infrastructure fund
- **Illinois Firefighters' Pension Investment Fund**: "Tiger Infrastructure Partners Fund IV", n/a, 2026-03-05, exact. Approved commitment to Tiger Infrastructure Partners Fund IV as Infrastructure Large Market Non-Core Fund

## Nordic Capital XII
`nordic capital||12`

- **Minnesota State Board of Investment**: "Nordic Capital XII", $150M, 2026-03-26, exact. Authorized commitment to Nordic Capital XII
- **New York City Retirement Systems (Comptroller)**: "Nordic Capital Fund XII", $126.8M, undated, exact. Private equity commitments closed subsequent to June 30, 2025 including Hg Saturn 4, Nordic Capital Fund XII, Reverence Capital Partners PE Opportunities Fund IV, and others totaling approximately $485 million
- **Orange County Employees Retirement System**: "Nordic Capital Fund XII", $50.7M, 2026-05-27, exact. Various new private equity fund commitments closed in 2025 including Nordic Capital Fund XII ($50.7M), Advent International GPE XI ($65.0M), Oak HC-FT Partners VI ($50.0M), HIG Europe Capital Partners IV ($41.1M), among others
