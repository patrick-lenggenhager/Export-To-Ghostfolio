import dayjs from "dayjs";
import { parse } from "csv-parse";
import { SecurityService } from "../securityService";
import { AbstractConverter } from "./abstractconverter";
import { BitpandaRecord } from "../models/bitpandaRecord";
import { GhostfolioExport } from "../models/ghostfolioExport";
import { GhostfolioOrderType } from "../models/ghostfolioOrderType";

export class BitpandaConverter extends AbstractConverter {
    /**
     * Converts grams to troy ounces.
     * 1 troy ounce = 31.1034768 grams
     */
    private gramsToTroyOunces(grams: number): number {
        return grams / 31.1034768;
    }
    /**
     * Maps Bitpanda asset symbols to Yahoo-compatible symbols.
     */
    private mapBitpandaSymbolToYahoo(asset: string): string {
        const mapping: Record<string, string> = {
            // Bitpanda symbol : Yahoo symbol
            "POL": "MATIC" // Polygon
        };
        return mapping[asset] || asset;
    }
    constructor(securityService: SecurityService) {
        super(securityService);
    }

    /**
     * @inheritdoc
     */
    public processFileContents(input: string, successCallback: any, errorCallback: any): void {
        // Parse the CSV and convert to Ghostfolio import format.
        // Preprocess input to ensure each data row has the correct number of columns
        const lines = input.split(/\r?\n/);
        const headerLineIdx = 7; // 0-based, line 8 is the header
        const expectedColumns = this.processHeaders(input).length;
        for (let i = headerLineIdx + 1; i < lines.length; i++) {
            if (!lines[i].trim()) continue;
            const cols = lines[i].split(",");
            if (cols.length < expectedColumns) {
                // Add missing columns as "-"
                lines[i] = lines[i] + Array(expectedColumns - cols.length).fill(",-").join("");
            }
        }
        const processedInput = lines.join("\n");
        parse(processedInput, {
            delimiter: ",",
            fromLine: 8, // skip header and meta lines
            columns: this.processHeaders(input),
            cast: (columnValue, context) => {
                // Parse numbers to floats (from string)
                const floatFields = [
                    "amountFiat", "amountAsset", "assetMarketPrice", "fee", "feePercent", "spread", "taxFiat"
                ];
                if (typeof context.column === "string" && floatFields.includes(context.column)) {
                    if (columnValue === "" || columnValue === "-") return 0;
                    // Remove currency symbols and parse
                    return Math.abs(parseFloat((columnValue + '').replace(/[^\d.-]/g, "")));
                }
                return columnValue;
            }
        }, async (err, records: BitpandaRecord[]) => {
            try {
                if (err || records === undefined || records.length === 0) {
                    let errorMsg = "An error occurred while parsing!";
                    if (err) errorMsg += ` Details: ${err.message}`;
                    return errorCallback(new Error(errorMsg));
                }
                console.log("[i] Read Bitpanda CSV file. Start processing..");
                const result: GhostfolioExport = {
                    meta: {
                        date: new Date(),
                        version: "v0"
                    },
                    activities: []
                };
                const bar1 = this.progress.create(records.length, 0);
                for (let idx = 0; idx < records.length; idx++) {
                    const record = records[idx];
                    if (this.isIgnoredRecord(record)) {
                        bar1.increment();
                        continue;
                    }
                    // Determine type
                    let type: keyof typeof GhostfolioOrderType | undefined = undefined;
                    if (record.transactionType.toLowerCase() === "reward") {
                        type = "interest";
                    } else if (record.transactionType.toLowerCase().startsWith("transfer")) {
                        type = record.inOut.toLowerCase() === "incoming" ? "buy" : "sell";
                    } else if (record.transactionType.toLowerCase() === "buy") {
                        type = "buy";
                    } else if (record.transactionType.toLowerCase() === "sell") {
                        type = "sell";
                    }
                    if (!type) {
                        bar1.increment();
                        continue;
                    }
                    // Convert symbol, amount, unit price based on asset class
                    let symbol: string | undefined = undefined;
                    const assetClass = record.assetClass ? record.assetClass.toLowerCase() : "";
                    let quantity = record.amountAsset;
                    let unitPrice = record.assetMarketPrice;
                    if (assetClass === "cryptocurrency") {
                            const yahooAsset = this.mapBitpandaSymbolToYahoo(record.asset);
                            symbol = `${yahooAsset}USD`;
                    } else if (assetClass === "stock (derivative)") {
                        symbol = record.asset;
                    } else if (assetClass === "metal") {
                        // Convert grams to troy ounces for both quantity and unit price
                        quantity = this.gramsToTroyOunces(quantity);
                        unitPrice = unitPrice * 31.1034768; // price per troy ounce
                        // Lookup symbol from Yahoo for metals
                        try {
                            const security = await this.securityService.getSecurity(
                                null,
                                null,
                                record.asset,
                                null,
                                this.progress
                            );
                            if (security && security.symbol) {
                                symbol = security.symbol;
                            } else {
                                console.log(`[i] No Yahoo symbol found for metal asset ${record.asset} (${record.assetMarketPriceCurrency})! Using asset name as fallback.\n`);
                                symbol = record.asset;
                            }
                        } catch (err) {
                            console.log(`[e] Error looking up Yahoo symbol for metal asset ${record.asset}: ${err.message}\n`);
                            symbol = record.asset;
                        }
                    } else {
                        symbol = record.asset;
                    }
                    // Parse date
                    const date = dayjs(record.timestamp);

                    // Add activities to result
                    if (type == "interest") {
                        // For staking reward transactions, two activities need to be added
                        // Reward in fiat currency
                        result.activities.push({
                            accountId: process.env.GHOSTFOLIO_ACCOUNT_ID,
                            comment: `Staking reward ${record.asset}`,
                            fee: record.fee,
                            quantity: 1,
                            type: GhostfolioOrderType[type],
                            unitPrice: record.amountFiat,
                            currency: record.fiat,
                            dataSource: "MANUAL",
                            date: date.format("YYYY-MM-DDTHH:mm:ssZ"),
                            symbol: process.env.GHOSTFOLIO_BITPANDA_STAKING_REWARD_ASSET_ID
                        });
                        // Fictitions buy transaction for cryptocurrency
                        result.activities.push({
                            accountId: process.env.GHOSTFOLIO_ACCOUNT_ID,
                            comment: "",
                            fee: 0,
                            quantity: quantity,
                            type: GhostfolioOrderType["buy"],
                            unitPrice: unitPrice,
                            currency: record.assetMarketPriceCurrency || record.fiat,
                            dataSource: "YAHOO",
                            date: date.format("YYYY-MM-DDTHH:mm:ssZ"),
                            symbol: symbol
                        });
                    } else {
                        // In all other cases, add the corresponding buy/sell activity
                        result.activities.push({
                            accountId: process.env.GHOSTFOLIO_ACCOUNT_ID,
                            comment: "",
                            fee: record.fee,
                            quantity: quantity,
                            type: GhostfolioOrderType[type],
                            unitPrice: unitPrice,
                            currency: record.assetMarketPriceCurrency || record.fiat,
                            dataSource: "YAHOO",
                            date: date.format("YYYY-MM-DDTHH:mm:ssZ"),
                            symbol: symbol
                        });
                    }
                    bar1.increment();
                }
                this.progress.stop();
                successCallback(result);
            } catch (error) {
                console.log("[e] An error occurred while processing the file contents. Stack trace:");
                console.log(error.stack);
                this.progress.stop();
                errorCallback(error);
            }
        });
    }

    /**
     * @inheritdoc
     */
    protected processHeaders(_: string): string[] {
        // Header mapping from Bitpanda CSV export (row 8)
        return [
            "transactionId",
            "timestamp",
            "transactionType",
            "inOut",
            "amountFiat",
            "fiat",
            "amountAsset",
            "asset",
            "assetMarketPrice",
            "assetMarketPriceCurrency",
            "assetClass",
            "productId",
            "fee",
            "feeAsset",
            "feePercent",
            "spread",
            "spreadCurrency",
            "taxFiat"
        ];
    }

    /**
     * @inheritdoc
     */
    public isIgnoredRecord(record: BitpandaRecord): boolean {
        // Exclude if assetClass is missing or is "Fiat"
        return !record.assetClass || record.assetClass.toLowerCase() === "fiat";
    }
}
