import fs from "node:fs/promises";
import path from "node:path";

import { Workbook, SpreadsheetFile } from "@oai/artifact-tool";

const rootDir = "C:/Users/rdiezl/Desktop/proyecto/market_basket";
const detailDir = path.join(rootDir, "output", "mahou_codex_rotacion_2026_rentabilidad_centro", "detail");
const manifestPath = path.join(detailDir, "workbook_tables_detail.json");
const workbookPath = path.join(detailDir, "excel_rentabilidad_centro_mahou_2026.xlsx");

const maxSheetNameLength = 31;

function columnName(index) {
  let n = index + 1;
  let result = "";
  while (n > 0) {
    const remainder = (n - 1) % 26;
    result = String.fromCharCode(65 + remainder) + result;
    n = Math.floor((n - 1) / 26);
  }
  return result;
}

function rangeForMatrix(rows, cols) {
  return `A1:${columnName(cols - 1)}${rows}`;
}

function sanitizeSheetName(name, used) {
  const cleaned = name.replace(/[\\/*?:[\]]/g, "_").slice(0, maxSheetNameLength);
  let candidate = cleaned || "Sheet";
  let suffix = 1;
  while (used.has(candidate)) {
    const trimmed = cleaned.slice(0, maxSheetNameLength - String(suffix).length - 1);
    candidate = `${trimmed}_${suffix}`;
    suffix += 1;
  }
  used.add(candidate);
  return candidate;
}

function toMatrix(records) {
  if (!records || records.length === 0) {
    return [["sin_datos"]];
  }
  const columns = Object.keys(records[0]);
  const dataRows = records.map((record) =>
    columns.map((column) => {
      const value = record[column];
      if (value === null || value === undefined) {
        return "";
      }
      if (typeof value === "object") {
        return JSON.stringify(value);
      }
      return value;
    }),
  );
  return [columns, ...dataRows];
}

const manifest = JSON.parse(await fs.readFile(manifestPath, "utf8"));
const workbook = Workbook.create();
const usedSheetNames = new Set(["Resumen"]);

const summarySheet = workbook.worksheets.add("Resumen");
const summaryRows = [
  ["artefacto", "ruta"],
  ["detail_dir", detailDir],
  ["summary_md", path.join(detailDir, "resumen_rentabilidad_centro_2026.md")],
  ["scenario_csv", path.join(detailDir, "tabla_escenarios_rentabilidad_centro_2026.csv")],
  ["sensitivity_csv", path.join(detailDir, "tabla_sensibilidad_salto_pasillo_2026.csv")],
  ["tables", Object.keys(manifest).length],
];
summarySheet.getRange(rangeForMatrix(summaryRows.length, summaryRows[0].length)).values = summaryRows;

for (const [tableName, records] of Object.entries(manifest)) {
  const sheetName = sanitizeSheetName(tableName, usedSheetNames);
  const matrix = toMatrix(records);
  const sheet = workbook.worksheets.add(sheetName);
  sheet.getRange(rangeForMatrix(matrix.length, matrix[0].length)).values = matrix;
}

await fs.mkdir(detailDir, { recursive: true });
const output = await SpreadsheetFile.exportXlsx(workbook);
await output.save(workbookPath);
console.log(workbookPath);
