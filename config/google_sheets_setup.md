# Google Sheets Setup (Distru Exports)

Use 3 tabs:

- `Inventory` (paste raw Distru Inventory Valuation export)
- `Sales` (paste raw Distru sales export with SKU + Quantity columns)
- `Priority` (formula-driven prioritized list)

## 1) Required columns

### Inventory tab
- `SKU`
- `Name`
- `Available Quantity`
- `Incoming Quantity`
- `Inventory Threshold Min` (optional but recommended)

### Sales tab
- `SKU`
- `Quantity` (or `Qty`, `Units Sold`, etc.)
- Date column (optional but recommended)

## 2) Priority tab headers

Put these headers in row 1:

`Priority Rank | Priority | SKU | Name | Available Qty | Incoming Qty | Threshold Min | Units Sold | Avg Daily Sales | Days of Cover | Target Days | Recommended Reorder Qty | Urgency Score`

Put target days in `N1` as text `Target Days`, and in `N2` enter `21`.

## 3) Build SKU list

In `C2`:

```gs
=SORT(UNIQUE(FILTER(Inventory!B:B, Inventory!B:B<>"")))
```

## 4) Lookup inventory fields

In `D2`:
```gs
=IF(C2="","",IFERROR(XLOOKUP(C2,Inventory!B:B,Inventory!A:A,""),""))
```

In `E2`:
```gs
=IF(C2="","",IFERROR(XLOOKUP(C2,Inventory!B:B,Inventory!L:L,0),0))
```

In `F2`:
```gs
=IF(C2="","",IFERROR(XLOOKUP(C2,Inventory!B:B,Inventory!M:M,0),0))
```

In `G2`:
```gs
=IF(C2="","",IFERROR(XLOOKUP(C2,Inventory!B:B,Inventory!R:R,0),0))
```

## 5) Aggregate sales by SKU

In `H2`:
```gs
=IF(C2="","",SUMIF(Sales!A:A,C2,Sales!B:B))
```

In `I2` (assumes rolling 30-day sales window):
```gs
=IF(C2="",,H2/30)
```

## 6) Coverage and reorder

In `J2`:
```gs
=IF(C2="","",IF(I2=0,"",E2/I2))
```

In `K2`:
```gs
=IF(C2="","",$N$2)
```

In `L2`:
```gs
=IF(C2="","",MAX(0,ROUNDUP((I2*$N$2)-(E2+F2),0)))
```

In `M2`:
```gs
=IF(C2="","",ROUND((MAX(0,MIN(1,($N$2-IFERROR(J2,$N$2))/$N$2))*70) + (IF(MAX($I$2:$I$999)=0,0,I2/MAX($I$2:$I$999))*20) + (IF(E2<G2,1,0)*10),2))
```

## 7) Priority bucket and rank

In `B2`:
```gs
=IF(C2="","",IF(L2<=0,"Low",IF(OR(J2<7,M2>=70),"Critical",IF(OR(J2<14,M2>=45),"High",IF(OR(J2<$N$2,M2>=25),"Medium","Low")))))
```

In `A2`:
```gs
=IF(C2="","",RANK(M2,FILTER($M$2:$M,$C$2:$C<>""),0))
```

Copy formulas down.

## Notes

- If your Sales export has different column positions, map your `SUMIF` range to actual SKU/Quantity columns.
- If your sales window is not 30 days, replace `30` in `I2`.
- This sheet is compatible with Google Sheets and can be rerun by replacing raw export tabs.

## Packaging rule alignment (Smalls -> 14g only)

If you add packaging recommendation columns in Sheets (outside this basic Priority tab), keep this rule aligned with the pipeline:

- Any Unit row with `Smalls` (or `Small Popcorn Buds`) should target `14g` only.

Example helper formula for a `Target Package Option` column (assuming `Name` is in `D2`):

```gs
=IF(REGEXMATCH(LOWER(D2),"smalls|small popcorn buds"),"14g",IF(REGEXMATCH(LOWER(D2),"14g|1/2 ounce|half ounce"),"14g","3.5g"))
```

Example helper formula for a `Packaging Next Action` column (assuming `Target Package Option` is in `O2` and available unit counts are in `P2` for 3.5g and `Q2` for 14g):

```gs
=IF(O2="14g",IF(P2>0,"Package 14g options next using available 3.5g inventory","Package 14g options next"),IF(O2="3.5g",IF(Q2>0,"Package 3.5g options next using available 14g inventory","Package 3.5g options next"),""))
```
