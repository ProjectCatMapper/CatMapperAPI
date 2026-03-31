# Explore Node Header And Category Info Layout Refinement 2026 03 28

Frontend detail-page layout refinements applied in `CatMapperJS`.

Changes:
- Increased the `Category Info` header title size to restore the stronger original hierarchy
- Moved the logs action into the header action cluster beside `Bookmark`
- Renamed `View Logs` to `Change Logs`
- Increased the typography for the `CMName` / `CMID` / `Domain` row and bolded `CMName`
- Allowed `language` and `religion` cards to use wider widths before wrapping
- Adjusted the `Location` row so it only consumes a single line when the value fits on one line

Files:
- `CatMapperJS/src/components/ExploreNode.js`
- `CatMapperJS/src/components/ExploreNode.css`

Verification:
- `npm run build` completed successfully in `CatMapperJS`
