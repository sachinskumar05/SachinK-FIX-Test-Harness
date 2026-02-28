# app-ui

React + Vite UI for the FIX replay harness.

## Run

```bash
npm install
npm run dev
```

Set backend base URL with `VITE_API_BASE_URL` (defaults to `http://localhost:7000`).

## Pages

- `Inputs`: scan source (path or upload), server-side path fields, YAML scenario editor.
- `Run`: trigger `/api/scan`, `/api/prepare`, `/api/run-offline`, `/api/run-online`.
- `Results`: summary cards, diff filters, diff table, side-by-side expected/actual tag view.

## Notes

- UI does not implement FIX business logic.
- UI only orchestrates REST calls to `app-server` and renders returned JSON.
