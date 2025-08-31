import dayjs from "dayjs";
import utc from "dayjs/plugin/utc";
import timezone from "dayjs/plugin/timezone";
import { parse } from "csv-parse";
import { AbstractConverter } from "./abstractconverter";
import { SecurityService } from "../securityService";
import { GhostfolioExport } from "../models/ghostfolioExport";
import YahooFinanceRecord from "../models/yahooFinanceRecord";
import { TrueWealthRecord } from "../models/truewealthRecord";
import { GhostfolioOrderType } from "../models/ghostfolioOrderType";
import { isNumberObject } from "util/types";

dayjs.extend(utc);
dayjs.extend(timezone);

export class TrueWealthConverter extends AbstractConverter {

    constructor(securityService: SecurityService) {
        super(securityService);
    }

    /**
     * @inheritdoc
     */
    public processFileContents(input: string, successCallback: any, errorCallback: any): void {

        // Parse the CSV and convert to Ghostfolio import format.
        parse(input, {
            delimiter: ",",
            fromLine: 2,
            columns: this.processHeaders(input, ","),
            cast: (columnValue, context) => {

                // Custom mapping below.

                // Convert categories to Ghostfolio type.
                if (context.column === "type") {
                    const action = columnValue.toLocaleLowerCase();

                    if (action.indexOf("buy") > -1) {
                        return "buy";
                    }
                    else if (action.indexOf("sell") > -1) {
                        return "sell";
                    }
                    else if (action.indexOf("dividend") > -1) {
                        return "dividend";
                    }
                    else if (action.indexOf("fx") > -1) {
                        return "fx";
                    }
                }

                // Parse numbers to floats (from string).
                if (context.column === "price" ||
                    context.column === "shares" ||
                    context.column === "amount" ||
                    context.column === "taxes" ||
                    context.column === "fees" ||
                    context.column === "value") {
                    return parseFloat(columnValue);
                }

                return columnValue;
            }
        }, async (err, records: TrueWealthRecord[]) => {

            // Check if parsing failed..
            if (err || records === undefined || records.length === 0) {
                let errorMsg = "An error ocurred while parsing!";

                if (err) {
                    errorMsg += ` Details: ${err.message}`
                }

                return errorCallback(new Error(errorMsg))
            }

            console.log("[i] Read CSV file. Start processing..");
            const result: GhostfolioExport = {
                meta: {
                    date: new Date(),
                    version: "v0"
                },
                activities: []
            }

            // Populate the progress bar.
            const bar1 = this.progress.create(records.length, 0);

            for (let idx = 0; idx < records.length; idx++) {
                const record = records[idx];

                // Check if the record should be ignored.
                if (this.isIgnoredRecord(record)) {
                    bar1.increment();
                    continue;
                }

                let security: YahooFinanceRecord;
                try {
                    security = await this.securityService.getSecurity(
                        record.isin,
                        null,
                        record.securityName,
                        record.currency,
                        this.progress);
                }
                catch (err) {
                    this.logQueryError(record.isin || record.securityName, idx + 2);
                    return errorCallback(err);
                }

                // Log whenever there was no match found.
                if (!security) {
                    this.progress.log(`[i] No result found for ${record.type} action for ${record.isin || record.securityName} with currency ${record.currency}! Please add this manually..\n`);
                    bar1.increment();
                    continue;
                }

                // Set fees to 0 if undefined.
                let fees = record.fees;
                if (isNumberObject(fees) === false) {
                    fees = 0;
                }

                // Make negative numbers (on sell records) absolute.
                let numberOfShares = Math.abs(record.shares);
                let assetPrice = Math.abs(record.price);

                // Dividend record values are retrieved from value.
                if (record.type === "dividend") {
                    numberOfShares = 1;
                    assetPrice = Math.abs(record.price);
                }

                // Parse date
                const date = dayjs.tz(`${record.date}`, "YYYY-MM-DD", "Europe/Zurich").hour(15);

                // Add record to export.
                result.activities.push({
                    accountId: process.env.GHOSTFOLIO_ACCOUNT_ID,
                    comment: "",
                    fee: fees,
                    quantity: numberOfShares,
                    type: GhostfolioOrderType[record.type],
                    unitPrice: assetPrice,
                    currency: security.currency ?? record.currency,
                    dataSource: "YAHOO",
                    date: date.format("YYYY-MM-DDTHH:mm:ssZ"),
                    symbol: security.symbol
                });

                bar1.increment();
            }

            this.progress.stop()

            successCallback(result);
        });
    }

    /**
     * @inheritdoc
     */
    public isIgnoredRecord(record: TrueWealthRecord): boolean {
        let ignoredRecordTypes = ["fx"];

        return ignoredRecordTypes.some(t => record.type.toLocaleLowerCase().indexOf(t) > -1)
    }
}
