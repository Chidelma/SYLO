import TTID from '@vyckr/ttid'

class Format {
  static table(docs: Record<string, any>) {
    // Calculate the _id column width (considering both the column name and the actual keys)
    const idColumnWidth =
      Math.max(...Object.keys(docs).map((key) => key.length)) + 2; // Add padding

    const { maxWidths, maxHeight } = this.getHeaderDim(Object.values(docs));

    let key = Object.keys(docs).shift()!

    const keys = key.split(',')

    if(TTID.isTTID(key) || keys.some(key => TTID.isTTID(key ?? ''))) {
      key = '_id'
    } else key = '_key'
    
    // Add the _id column to the front of maxWidths
    const fullWidths = {
      [key]: idColumnWidth,
      ...maxWidths,
    };

    // Render the header
    const header = this.renderHeader(fullWidths, maxHeight, key);
    console.log("\n" + header);

    // Render the data rows
    const dataRows = this.renderDataRows(docs, fullWidths, key);
    console.log(dataRows);
  }

  private static getHeaderDim(docs: Record<string, any>[]) {
    let maxWidths: Record<string, any> = {};
    let maxHeight = 1;

    // Create a copy to avoid mutating the original array
    const docsCopy = [...docs];

    while (docsCopy.length > 0) {
      const doc = docsCopy.shift()!;
      const widths = this.getValueWidth(doc);
      const height = this.getHeaderHeight(doc); // Fix: get height for this doc
      maxHeight = Math.max(maxHeight, height); // Fix: take maximum height
      maxWidths = this.increaseWidths(maxWidths, widths);
    }

    return { maxWidths, maxHeight };
  }

  private static getValueWidth(doc: Record<string, any>) {
    const keyWidths: Record<string, any> = {};

    for (const key in doc) {
      if (
        typeof doc[key] === "object" &&
        doc[key] !== null &&
        !Array.isArray(doc[key])
      ) {
        keyWidths[key] = this.getValueWidth(doc[key]);
      } else {
        // Consider both the key name length and the value length
        const valueWidth = JSON.stringify(doc[key]).length;
        const keyWidth = key.length;
        // Add padding: 1 space before + content + 1 space after
        keyWidths[key] = Math.max(valueWidth, keyWidth) + 2;
      }
    }

    return keyWidths;
  }

  private static increaseWidths(
    oldWidths: Record<string, any>,
    newWidths: Record<string, any>
  ) {
    const increasedWidths: Record<string, any> = { ...oldWidths };

    for (const key in newWidths) {
      if (
        oldWidths[key] &&
        typeof newWidths[key] === "object" &&
        typeof oldWidths[key] === "object"
      ) {
        increasedWidths[key] = this.increaseWidths(
          oldWidths[key],
          newWidths[key]
        );
      } else if (
        oldWidths[key] &&
        typeof newWidths[key] === "number" &&
        typeof oldWidths[key] === "number"
      ) {
        increasedWidths[key] = Math.max(newWidths[key], oldWidths[key]);
      } else {
        increasedWidths[key] = newWidths[key];
      }
    }

    // Handle keys that exist in newWidths but not in oldWidths
    for (const key in newWidths) {
      if (!(key in increasedWidths)) {
        increasedWidths[key] = newWidths[key];
      }
    }

    // Also ensure column family names fit within their total width
    for (const key in increasedWidths) {
      if (
        typeof increasedWidths[key] === "object" &&
        increasedWidths[key] !== null
      ) {
        const totalChildWidth = this.calculateTotalWidth(increasedWidths[key]);
        const keyWidth = key.length;

        // If the column family name (with padding) is longer than the total child width,
        // we need to adjust the child column widths proportionally
        const keyWidthWithPadding = keyWidth + 2; // Add padding for family name too
        if (keyWidthWithPadding > totalChildWidth) {
          const childKeys = Object.keys(increasedWidths[key]);
          const extraWidth = keyWidthWithPadding - totalChildWidth;
          const widthPerChild = Math.ceil(extraWidth / childKeys.length);

          for (const childKey of childKeys) {
            if (typeof increasedWidths[key][childKey] === "number") {
              increasedWidths[key][childKey] += widthPerChild;
            }
          }
        }
      }
    }

    return increasedWidths;
  }

  private static getHeaderHeight(doc: Record<string, any>): number {
    let maxDepth = 1; // Fix: start with 1 for current level

    for (const key in doc) {
      if (
        typeof doc[key] === "object" &&
        doc[key] !== null &&
        !Array.isArray(doc[key])
      ) {
        const nestedDepth = 1 + this.getHeaderHeight(doc[key]); // Fix: add 1 for current level
        maxDepth = Math.max(maxDepth, nestedDepth); // Fix: track maximum depth
      }
    }

    return maxDepth;
  }

  private static renderHeader(
    widths: Record<string, any>,
    height: number,
    idColumnKey: string
  ): string {
    const lines: string[] = [];

    // Flatten the structure to get all columns
    const columns = this.flattenColumns(widths);

    // Add top border
    lines.push(this.renderTopBorder(columns));

    // Add header content rows
    for (let level = 0; level < height; level++) {
      lines.push(this.renderHeaderRow(widths, level, height, idColumnKey));

      // Add middle border between levels (except after last level)
      if (level < height - 1) {
        lines.push(this.renderMiddleBorder(columns));
      }
    }

    // Add bottom border
    lines.push(this.renderBottomBorder(columns));

    return lines.join("\n");
  }

  private static renderDataRows<T extends Record<string, any>>(
    docs: Record<string, T>,
    widths: Record<string, any>,
    idColumnKey: string
  ): string {
    const lines: string[] = [];
    const columns = this.flattenColumns(widths);

    for (const [docId, doc] of Object.entries(docs)) {
      // Render data row
      lines.push(this.renderDataRow(docId, doc, widths, columns, idColumnKey));

      // Add separator between rows (except for last row)
      const entries = Object.entries(docs);
      const isLastRow = entries[entries.length - 1][0] === docId;
      if (!isLastRow) {
        lines.push(this.renderRowSeparator(columns));
      }
    }

    // Add final bottom border
    lines.push(this.renderBottomBorder(columns));

    return lines.join("\n");
  }

  private static renderDataRow(
    docId: string,
    doc: Record<string, any>,
    widths: Record<string, any>,
    columns: Array<{ name: string; width: number; path: string[] }>,
    idColumnKey: string
  ): string {
    let line = "│";

    // Handle the ID column (could be _id or another key)
    if (idColumnKey in widths && typeof widths[idColumnKey] === "number") {
      const contentWidth = widths[idColumnKey] - 2;
      const content = docId;
      const padding = Math.max(0, contentWidth - content.length);
      const leftPad = Math.floor(padding / 2);
      const rightPad = padding - leftPad;

      line += " " + " ".repeat(leftPad) + content + " ".repeat(rightPad) + " │";
    }

    // Handle data columns
    for (const column of columns) {
      // Skip the ID column as it's handled separately
      if (column.name === idColumnKey) continue;

      const value = this.getNestedValue(doc, column.path);
      const stringValue = this.formatValue(value);
      const contentWidth = column.width - 2; // Subtract padding

      // Truncate if value is too long
      const truncatedValue =
        stringValue.length > contentWidth
          ? stringValue.substring(0, contentWidth - 3) + "..."
          : stringValue;

      const padding = Math.max(0, contentWidth - truncatedValue.length);
      const leftPad = Math.floor(padding / 2);
      const rightPad = padding - leftPad;

      line +=
        " " +
        " ".repeat(leftPad) +
        truncatedValue +
        " ".repeat(rightPad) +
        " │";
    }

    return line;
  }

  private static getNestedValue(obj: Record<string, any>, path: string[]): any {
    let current = obj;

    for (const key of path) {
      if (
        current === null ||
        current === undefined ||
        typeof current !== "object"
      ) {
        return undefined;
      }
      current = current[key];
    }

    return current;
  }

  private static formatValue(value: any): string {
    if (value === null) return "null";
    if (value === undefined) return "";
    if (Array.isArray(value)) return JSON.stringify(value);
    if (typeof value === "object") return JSON.stringify(value);
    if (typeof value === "string") return value;
    return String(value);
  }

  private static renderRowSeparator(
    columns: Array<{ name: string; width: number; path: string[] }>
  ): string {
    let line = "├";

    for (let i = 0; i < columns.length; i++) {
      line += "─".repeat(columns[i].width);
      if (i < columns.length - 1) {
        line += "┼";
      }
    }

    line += "┤";
    return line;
  }

  private static flattenColumns(
    widths: Record<string, any>,
    path: string[] = []
  ): Array<{ name: string; width: number; path: string[] }> {
    const columns: Array<{ name: string; width: number; path: string[] }> = [];

    for (const key in widths) {
      const currentPath = [...path, key];

      if (typeof widths[key] === "object" && widths[key] !== null) {
        // Recursively flatten nested objects
        columns.push(...this.flattenColumns(widths[key], currentPath));
      } else {
        // This is a leaf column
        columns.push({
          name: key,
          width: widths[key],
          path: currentPath,
        });
      }
    }

    return columns;
  }

  private static renderHeaderRow(
    widths: Record<string, any>,
    currentLevel: number,
    totalHeight: number,
    idColumnKey: string
  ): string {
    let line = "│";

    // Handle the ID column specially (could be _id or another key)
    if (idColumnKey in widths && typeof widths[idColumnKey] === "number") {
      if (currentLevel === 0) {
        // Show the ID column header at the top level
        const contentWidth = widths[idColumnKey] - 2;
        const headerText = idColumnKey === '_id' ? '_id' : idColumnKey;
        const padding = Math.max(0, contentWidth - headerText.length);
        const leftPad = Math.floor(padding / 2);
        const rightPad = padding - leftPad;

        line += " " + " ".repeat(leftPad) + headerText + " ".repeat(rightPad) + " │";
      } else {
        // Empty cell for other levels
        line += " ".repeat(widths[idColumnKey]) + "│";
      }
    }

    const processLevel = (
      obj: Record<string, any>,
      level: number,
      targetLevel: number
    ): string => {
      let result = "";

      for (const key in obj) {
        // Skip the ID column as it's handled separately
        if (key === idColumnKey) continue;

        if (typeof obj[key] === "object" && obj[key] !== null) {
          if (level === targetLevel) {
            // This is a column family at the target level
            const totalWidth = this.calculateTotalWidth(obj[key]);
            const contentWidth = totalWidth - 2; // Subtract padding
            const padding = Math.max(0, contentWidth - key.length);
            const leftPad = Math.floor(padding / 2);
            const rightPad = padding - leftPad;

            // Add 1 space padding + centered content + 1 space padding
            result +=
              " " + " ".repeat(leftPad) + key + " ".repeat(rightPad) + " │";
          } else if (level < targetLevel) {
            // Recurse deeper
            result += processLevel(obj[key], level + 1, targetLevel);
          }
        } else {
          if (level === targetLevel) {
            // This is a leaf column at the target level
            const contentWidth = obj[key] - 2; // Subtract padding
            const padding = Math.max(0, contentWidth - key.length);
            const leftPad = Math.floor(padding / 2);
            const rightPad = padding - leftPad;

            // Add 1 space padding + centered content + 1 space padding
            result +=
              " " + " ".repeat(leftPad) + key + " ".repeat(rightPad) + " │";
          } else if (level < targetLevel) {
            // Empty cell - span the full width
            result += " ".repeat(obj[key]) + "│";
          }
        }
      }

      return result;
    };

    line += processLevel(widths, 0, currentLevel);
    return line;
  }

  private static calculateTotalWidth(obj: Record<string, any>): number {
    let total = 0;
    let columnCount = 0;

    for (const key in obj) {
      if (typeof obj[key] === "object" && obj[key] !== null) {
        total += this.calculateTotalWidth(obj[key]);
        columnCount += this.countLeafColumns(obj[key]);
      } else {
        total += obj[key];
        columnCount++;
      }
    }

    // Add space for separators between columns (one less than column count)
    return total + Math.max(0, columnCount - 1);
  }

  private static countLeafColumns(obj: Record<string, any>): number {
    let count = 0;

    for (const key in obj) {
      if (typeof obj[key] === "object" && obj[key] !== null) {
        count += this.countLeafColumns(obj[key]);
      } else {
        count++;
      }
    }

    return count;
  }

  private static renderTopBorder(
    columns: Array<{ name: string; width: number; path: string[] }>
  ): string {
    let line = "┌";

    for (let i = 0; i < columns.length; i++) {
      line += "─".repeat(columns[i].width);
      if (i < columns.length - 1) {
        line += "┬";
      }
    }

    line += "┐";
    return line;
  }

  private static renderMiddleBorder(
    columns: Array<{ name: string; width: number; path: string[] }>
  ): string {
    let line = "├";

    for (let i = 0; i < columns.length; i++) {
      line += "─".repeat(columns[i].width);
      if (i < columns.length - 1) {
        line += "┼";
      }
    }

    line += "┤";
    return line;
  }

  private static renderBottomBorder(
    columns: Array<{ name: string; width: number; path: string[] }>
  ): string {
    let line = "└";

    for (let i = 0; i < columns.length; i++) {
      line += "─".repeat(columns[i].width);
      if (i < columns.length - 1) {
        line += "┴";
      }
    }

    line += "┘";
    return line;
  }
}

console.format = function(docs: Record<string, any>) {
  Format.table(docs)
}

Object.appendGroup = function (target: Record<string, any>, source: Record<string, any>) {
  // TODO: implement mergeGroup logic
  const result = { ...target }
        
  for (const [sourceId, sourceGroup] of Object.entries(source)) {

      if(!result[sourceId]) {
          result[sourceId] = sourceGroup
          break
      }

      for(const [groupId, groupDoc] of Object.entries(sourceGroup)) {
          result[sourceId][groupId] = groupDoc
      }
  }
  
  return result
}