export class BitpandaRecord {
    transactionId: string;
    timestamp: string;
    transactionType: string;
    inOut: string;
    amountFiat: number;
    fiat: string;
    amountAsset: number;
    asset: string;
    assetMarketPrice: number;
    assetMarketPriceCurrency: string;
    assetClass: string;
    productId: string;
    fee: number;
    feeAsset: string;
    feePercent: number;
    spread: number;
    spreadCurrency: string;
    taxFiat: number;
}
