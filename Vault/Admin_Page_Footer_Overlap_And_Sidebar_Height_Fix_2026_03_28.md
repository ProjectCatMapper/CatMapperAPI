# Admin Page Footer Overlap And Sidebar Height Fix 2026 03 28

Frontend layout fix applied in `CatMapperJS`.

Issue:
- The admin page footer overlapped page content
- The page used nested `100vh` containers and internal scroll regions, so the document did not expand to fit all sidebar items

Fix:
- Changed the admin route wrapper from fixed `height: 100vh` to `minHeight: 100vh`
- Removed fixed full-viewport height from the admin component root
- Removed internal `overflowY: auto` scrolling on the sidebar list and main admin content pane so the page can grow naturally with content

Files:
- `CatMapperJS/src/routes/Admin.js`
- `CatMapperJS/src/components/Admin.js`

Verification:
- `npm run build` completed successfully in `CatMapperJS`
