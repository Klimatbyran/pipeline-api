import { Pipeline } from "../schemas/types";

export const pipeline: Pipeline = [
    {
        id: "nlmParsePDF",
        name: "NLM PDF Parsing",
        next: {
            selection: true,
            target: ["doclingParsePDF", "nlmExtractTables", "precheck"]
        }
    },
    {
        id: "doclingParsePDF",
        name: "Docling PDF Parsing",
        next: {
            target: ["indexMarkdown"]
        }
    },
    {
        id: "nlmExtractTables",
        name: "Table Extraction",
        next: {
            target: ["indexMarkdown"]
        }
    },
    {
        id: "indexMarkdown",
        name: "Markdown",
        next: {
            target: ["precheck"]
        }
    },
    {
        id: "precheck",
        name: "Pre-check",
        next: {
            target: ["guessWikidata", "followUpFiscalYear", "followUpReportYear"]
        }
    },
    {
        id: "guessWikidata",
        name: "Wikidata",
        next: {
            target: ["extractEmissions"]
        }
    },
    {
        id: "extractEmissions",
        name: "Emissions Data",
        next: {
            target: ["followUpScope12", "followUpScope3", "followUpBiogenic", "followUpEconomy", "followUpGoals", "followUpInitiatives", "followUpIndustryGics", "followUpBaseYear", "followUpCompanyTags", "followUpReportType"]
        }
    },
    {
        id: "followUpScope12",
        name: "Follow-up Scope 1+2",
        next: {
            target: ["checkDB"]
        }
    },
    {
        id: "followUpScope3",
        name: "Follow-up Scope 3",
        next: {
            target: ["checkDB"]
        }
    },
    {
        id: "followUpBiogenic",
        name: "Follow-up Biogenic",
        next: {
            target: ["checkDB"]
        }
    },
    {
        id: "followUpEconomy",
        name: "Follow-up Economy",
        next: {
            target: ["checkDB"]
        }
    },
    {
        id: "followUpGoals",
        name: "Follow-up Goals",
        next: {
            target: ["checkDB"]
        }
    },
    {
        id: "followUpInitiatives",
        name: "Follow-up Initiatives",
        next: {
            target: ["checkDB"]
        }
    },
    {
        id: "followUpFiscalYear",
        name: "Follow-up Fiscal Year",
        next: {
            target: ["extractEmissions"]
        }
    },
    {
        id: "followUpReportYear",
        name: "Follow-up Report Year",
        next: {
            target: ["extractEmissions"]
        }
    },
    {
        id: "followUpCompanyTags",
        name: "Follow-up Company Tags"
    },
    {
        id: "followUpReportType",
        name: "Follow-up Report Type"
    },
    {
        id: "followUpBaseYear",
        name: "Follow-up Base Year",
        next: {
            target: ["checkDB"]
        }
    },
    {
        id: "followUpIndustryGics",
        name: "Follow-up Industry GICS",
        next: {
            target: ["checkDB"]
        }
    },
    {
        id: "checkDB",
        name: "DB Check",
        next: {
            target: ["diffIndustry", "diffGoals", "diffInitiatives", "diffBaseYear", "diffReportingPeriods", "diffTags", "diffReportType"]
        }
    },
    {
        id: "diffIndustry",
        name: "Industry",
        next: {
            target: ["saveToAPI", "sendCompanyLink"]
        }
    },
    {
        id: "diffGoals",
        name: "Climate Goals",
        next: {
            target: ["saveToAPI", "sendCompanyLink"]
        }
    },
    {
        id: "diffInitiatives",
        name: "Initiatives",
        next: {
            target: ["saveToAPI", "sendCompanyLink"]
        }
    },
    {
        id: "diffBaseYear",
        name: "Base Year",
        next: {
            target: ["saveToAPI", "sendCompanyLink"]
        }
    },
    {
        id: "diffReportingPeriods",
        name: "Financial Years",
        next: {
            target: ["saveToAPI", "sendCompanyLink", "wikipediaUpload"]
        }
    },
    {
        id: "diffTags",
        name: "Tags",
        next: {
            target: ["saveToAPI", "sendCompanyLink"]
        }
    },
    {
        id: "diffReportType",
        name: "Report Type",
        next: {
            target: ["saveToAPI", "sendCompanyLink"]
        }
    },
    {
        id: "saveToAPI",
        name: "API Storage"
    },
    {
        id: "wikipediaUpload",
        name: "Wikipedia"
    },
    {
        id: "sendCompanyLink",
        name: "Review"
    }
]