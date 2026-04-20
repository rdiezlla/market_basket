import fs from "node:fs/promises";
import path from "node:path";

import { Workbook, SpreadsheetFile } from "@oai/artifact-tool";

const rootDir = "C:/Users/rdiezl/Desktop/proyecto/market_basket";
const outputDir = path.join(rootDir, "output", "mahou_codex");
const supportDir = path.join(outputDir, "support");
const manifestPath = path.join(supportDir, "workbook_tables.json");
const workbookPath = path.join(outputDir, "excel_maestro_mahou_codex.xlsx");

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
const usedSheetNames = new Set();

const summarySheet = workbook.worksheets.add("Resumen");
const summaryRows = [
  ["artefacto", "ruta"],
  ["csv_dir", path.join(outputDir, "csv")],
  ["plot_dir", path.join(outputDir, "plots")],
  ["resumen_codex", path.join(outputDir, "resumen_codex.md")],
  ["tablas", Object.keys(manifest).length],
];
summarySheet.getRange(rangeForMatrix(summaryRows.length, summaryRows[0].length)).values = summaryRows;

for (const [tableName, records] of Object.entries(manifest)) {
  const sheetName = sanitizeSheetName(tableName, usedSheetNames);
  const matrix = toMatrix(records);
  const sheet = workbook.worksheets.add(sheetName);
  sheet.getRange(rangeForMatrix(matrix.length, matrix[0].length)).values = matrix;
}

await fs.mkdir(outputDir, { recursive: true });
const output = await SpreadsheetFile.exportXlsx(workbook);
await output.save(workbookPath);
console.log(workbookPath);
