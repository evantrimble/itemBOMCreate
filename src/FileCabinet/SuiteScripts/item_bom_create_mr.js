/**
 * @NApiVersion 2.1
 * @NScriptType MapReduceScript
 * @NModuleScope SameAccount
 */

/**
 * BOM Import Map/Reduce Script v3
 * 
 * Purpose: Import items, BOMs, and BOM revisions from CSV file using hierarchy notation
 * 
 * Features:
 * - Hierarchy-based BOM structure (1.0, 1.1, 1.1.1 notation)
 * - Automatic type detection (assembly vs. inventory based on hierarchy)
 * - MRP setup with Planning Item Category, varied lead times, lot sizing
 * - Optional vendor creation from CSV
 * - Duplicate handling (items created once, linked to multiple BOMs)
 * - IDEMPOTENT: Re-runnable to complete partial imports
 *   - Checks/creates locations on existing items
 *   - Links BOMs to assembly items
 * 
 * Script Parameters:
 * - custscript_bom_config_file_id: Internal ID of the JSON config file
 */

define(['N/record', 'N/search', 'N/file', 'N/runtime', 'N/cache', 'N/format'],
    function(record, search, file, runtime, cache, format) {

        // Default configuration (fallbacks)
        const DEFAULT_CONFIG = {
            locationIds: [2, 13],
            subsidiaryId: 2,  // USA subsidiary
            vendorId: 625,
            purchasePrice: 1,
            taxScheduleId: 1,
            setupMRP: true,
            createVendors: false,
            itemLocationDefaults: {
                preferredstocklevel: 1000,
                reorderpoint: 600,
                safetystocklevel: 100,
                leadtime: 7
            }
        };

        // MRP Rotation values for demo variety
        const MRP_ROTATION = {
            leadTimes: [5, 14, 30, 100],
            lotSizingMethods: ['LOT_FOR_LOT', 'FIXED_LOT_MULTIPLE', 'PERIODS_OF_SUPPLY'],
            moqValues: [10, 25, 50, 100],
            fixedLotMultiple: 10,
            periodicLotSizeDays: 7
        };

        // MRP Location-level settings for demo variety
        const MRP_LOCATION_ROTATION = {
            leadTimes: [5, 14, 30, 100],
            lotSizingMethods: ['LOT_FOR_LOT', 'FIXED_LOT_SIZE', 'FIXED_LOT_MULTIPLE', 'PERIODIC_LOT_SIZE'],
            fixedLotSize: 100,
            fixedLotMultiple: 25,
            periodicLotSizeDays: 7,
            periodicLotSizeType: 'WEEKLY'
        };

        /**
         * GET INPUT DATA
         */
        function getInputData() {
            try {
                log.audit('========================================', '');
                log.audit('GET INPUT DATA - Starting', '');
                log.audit('========================================', '');

                // Load config file
                const scriptObj = runtime.getCurrentScript();
                const configFileId = scriptObj.getParameter({ name: 'custscript_bom_config_file_id' });

                if (!configFileId) {
                    throw new Error('Config file ID parameter is required');
                }

                log.audit('Loading Config', 'Config File ID: ' + configFileId);

                const configFile = file.load({ id: configFileId });
                const config = JSON.parse(configFile.getContents());

                log.audit('Config Loaded', JSON.stringify({
                    prospectName: config.prospectName,
                    csvFileId: config.csvFileId,
                    mappings: config.mappings,
                    defaults: config.defaults
                }));

                // Set defaults from config or use fallbacks
                let DEFAULTS = config.defaults || DEFAULT_CONFIG;
                
                // Ensure all required properties exist
                DEFAULTS.locationIds = DEFAULTS.locationIds || DEFAULT_CONFIG.locationIds;
                DEFAULTS.subsidiaryId = DEFAULTS.subsidiaryId || DEFAULT_CONFIG.subsidiaryId;
                DEFAULTS.vendorId = DEFAULTS.vendorId !== undefined ? DEFAULTS.vendorId : DEFAULT_CONFIG.vendorId;
                DEFAULTS.purchasePrice = DEFAULTS.purchasePrice || DEFAULT_CONFIG.purchasePrice;
                DEFAULTS.taxScheduleId = DEFAULTS.taxScheduleId || DEFAULT_CONFIG.taxScheduleId;
                DEFAULTS.setupMRP = DEFAULTS.setupMRP !== undefined ? DEFAULTS.setupMRP : DEFAULT_CONFIG.setupMRP;
                DEFAULTS.createVendors = DEFAULTS.createVendors !== undefined ? DEFAULTS.createVendors : DEFAULT_CONFIG.createVendors;
                DEFAULTS.itemLocationDefaults = DEFAULTS.itemLocationDefaults || DEFAULT_CONFIG.itemLocationDefaults;

                log.audit('Defaults Applied', JSON.stringify(DEFAULTS));

                // If MRP is enabled, create Planning Item Category
                let planningItemCategoryId = null;
                if (DEFAULTS.setupMRP) {
                    planningItemCategoryId = createOrGetPlanningItemCategory(config.prospectName);
                    log.audit('Planning Item Category', 'ID: ' + planningItemCategoryId);
                }

                // If Create Vendors is enabled, pre-create vendors from CSV
                let vendorCache = {};
                if (DEFAULTS.createVendors) {
                    vendorCache = createVendorsFromCSV(config.csvFileId, config.mappings, DEFAULTS);
                    log.audit('Vendors Created', JSON.stringify(vendorCache));
                }

                // Load CSV file
                const csvFile = file.load({ id: config.csvFileId });
                const csvContent = csvFile.getContents();
                const lines = csvContent.split('\n').filter(line => line.trim());

                if (lines.length < 2) {
                    throw new Error('CSV must have at least a header row and one data row');
                }

                // Parse headers (first row)
                const headers = parseRow(lines[0]);

                // Parse data rows
                const allRows = [];
                for (let i = 1; i < lines.length; i++) {
                    const rowData = parseRow(lines[i]);
                    if (rowData.some(cell => cell.trim())) {
                        allRows.push({
                            rowNumber: i + 1,
                            cells: rowData
                        });
                    }
                }

                log.audit('CSV Parsed', 'Headers: ' + headers.length + ', Data Rows: ' + allRows.length);

                // Map rows to fields using config
                const mappedRows = allRows.map(row => {
                    const mapped = {
                        rowNumber: row.rowNumber,
                        hierarchy: null,
                        itemFields: {},
                        bomFields: {},
                        vendorName: null
                    };

                    Object.keys(config.mappings).forEach(colIndex => {
                        const fieldName = config.mappings[colIndex];
                        const value = row.cells[parseInt(colIndex)] || '';

                        if (!value.trim()) return;

                        if (fieldName === 'hierarchy') {
                            mapped.hierarchy = value.trim();
                        } else if (fieldName === 'quantity') {
                            mapped.bomFields.quantity = parseFloat(value) || 1;
                        } else if (fieldName === 'memo') {
                            mapped.bomFields.memo = value.trim();
                        } else if (fieldName === 'vendor') {
                            mapped.vendorName = value.trim();
                        } else if (fieldName === 'displayname') {
                            // Map to all three description fields
                            mapped.itemFields.displayname = value.trim();
                            mapped.itemFields.salesdescription = value.trim();
                            mapped.itemFields.purchasedescription = value.trim();
                        } else {
                            // Item field
                            mapped.itemFields[fieldName] = value.trim();
                        }
                    });

                    return mapped;
                }).filter(row => row.hierarchy && row.itemFields.itemid);

                log.audit('Rows Mapped', mappedRows.length + ' valid rows with hierarchy and itemid');

                // Determine which rows are assemblies based on hierarchy
                const hierarchySet = new Set(mappedRows.map(r => r.hierarchy));
                
                // Track inventory item index for MRP rotation
                let inventoryIndex = 0;

                mappedRows.forEach(row => {
                    // Check if any other hierarchy starts with this one + "."
                    const isAssembly = Array.from(hierarchySet).some(h => 
                        h !== row.hierarchy && h.startsWith(row.hierarchy + '.')
                    );
                    row.isAssembly = isAssembly;
                    row.recordType = isAssembly ? 'assemblyitem' : 'inventoryitem';

                    // Assign MRP rotation index for inventory items
                    if (!isAssembly) {
                        row.mrpRotationIndex = inventoryIndex++;
                    }

                    // Determine parent hierarchy
                    const parts = row.hierarchy.split('.');
                    if (parts.length > 1) {
                        parts.pop();
                        row.parentHierarchy = parts.join('.');
                    } else {
                        row.parentHierarchy = null;
                    }
                });

                // Log type breakdown
                const inventoryCount = mappedRows.filter(r => !r.isAssembly).length;
                const assemblyCount = mappedRows.filter(r => r.isAssembly).length;
                log.audit('Type Breakdown', 'Inventory: ' + inventoryCount + ', Assembly: ' + assemblyCount);

                // Store all rows in cache for summarize stage
                const importCache = cache.getCache({
                    name: 'BOM_IMPORT_CACHE',
                    scope: cache.Scope.PRIVATE
                });

                importCache.put({
                    key: 'ALL_ROWS',
                    value: JSON.stringify(mappedRows),
                    ttl: 7200
                });

                // Add prospect name, defaults, and other config to each row
                mappedRows.forEach(row => {
                    row.prospectName = config.prospectName;
                    row.defaults = DEFAULTS;
                    row.planningItemCategoryId = planningItemCategoryId;
                    row.vendorCache = vendorCache;
                });

                log.audit('GET INPUT DATA - Complete', 'Returning ' + mappedRows.length + ' rows');

                return mappedRows;

            } catch (e) {
                log.error('getInputData Error', e.toString() + '\n' + e.stack);
                throw e;
            }
        }

        /**
         * Create or get existing Planning Item Category
         */
        function createOrGetPlanningItemCategory(prospectName) {
            try {
                const categorySearch = search.create({
                    type: 'planningitemcategory',
                    filters: [['name', 'is', prospectName]],
                    columns: ['internalid']
                });

                const results = categorySearch.run().getRange({ start: 0, end: 1 });
                
                if (results.length > 0) {
                    log.debug('Planning Item Category Exists', 'Name: ' + prospectName);
                    return results[0].getValue('internalid');
                }

                const categoryRec = record.create({
                    type: 'planningitemcategory',
                    isDynamic: true
                });

                categoryRec.setValue({ fieldId: 'name', value: prospectName });

                const categoryId = categoryRec.save();
                log.audit('Planning Item Category Created', 'Name: ' + prospectName + ', ID: ' + categoryId);

                return categoryId;

            } catch (e) {
                log.error('Planning Item Category Error', e.toString());
                return null;
            }
        }

        /**
         * Create vendors from CSV data
         */
        function createVendorsFromCSV(csvFileId, mappings, defaults) {
            const vendorCache = {};

            try {
                let vendorColIndex = null;
                Object.keys(mappings).forEach(colIndex => {
                    if (mappings[colIndex] === 'vendor') {
                        vendorColIndex = parseInt(colIndex);
                    }
                });

                if (vendorColIndex === null) {
                    log.debug('No Vendor Column', 'Vendor column not mapped');
                    return vendorCache;
                }

                const csvFile = file.load({ id: csvFileId });
                const csvContent = csvFile.getContents();
                const lines = csvContent.split('\n').filter(line => line.trim());

                const vendorNames = new Set();
                for (let i = 1; i < lines.length; i++) {
                    const rowData = parseRow(lines[i]);
                    const vendorName = rowData[vendorColIndex];
                    if (vendorName && vendorName.trim()) {
                        vendorNames.add(vendorName.trim());
                    }
                }

                log.audit('Unique Vendors Found', Array.from(vendorNames).join(', '));

                vendorNames.forEach(vendorName => {
                    try {
                        const vendorSearch = search.create({
                            type: 'vendor',
                            filters: [['entityid', 'is', vendorName]],
                            columns: ['internalid']
                        });

                        const results = vendorSearch.run().getRange({ start: 0, end: 1 });

                        if (results.length > 0) {
                            vendorCache[vendorName] = results[0].getValue('internalid');
                            log.debug('Vendor Exists', vendorName + ' (ID: ' + vendorCache[vendorName] + ')');
                        } else {
                            const vendorRec = record.create({
                                type: 'vendor',
                                isDynamic: true
                            });

                            vendorRec.setValue({ fieldId: 'companyname', value: vendorName });
                            vendorRec.setValue({ fieldId: 'subsidiary', value: defaults.vendorSubsidiaryId || DEFAULT_CONFIG.subsidiaryId });

                            const vendorId = vendorRec.save();
                            vendorCache[vendorName] = vendorId;
                            log.audit('Vendor Created', vendorName + ' (ID: ' + vendorId + ')');
                        }
                    } catch (e) {
                        log.error('Vendor Creation Error', vendorName + ': ' + e.toString());
                    }
                });

            } catch (e) {
                log.error('createVendorsFromCSV Error', e.toString());
            }

            return vendorCache;
        }

        /**
         * Parse CSV row
         */
        function parseRow(rowText) {
            const values = [];
            let current = '';
            let inQuotes = false;

            for (let i = 0; i < rowText.length; i++) {
                const char = rowText[i];
                const nextChar = rowText[i + 1];

                if (char === '"') {
                    if (inQuotes && nextChar === '"') {
                        current += '"';
                        i++;
                    } else {
                        inQuotes = !inQuotes;
                    }
                } else if ((char === ',' || char === '\t') && !inQuotes) {
                    values.push(current);
                    current = '';
                } else if (char !== '\r') {
                    current += char;
                }
            }
            values.push(current);

            return values;
        }

        /**
         * MAP - Classify and emit records
         */
        function map(context) {
            try {
                const rowData = JSON.parse(context.value);

                if (rowData.isAssembly) {
                    context.write('2_assembly', context.value);
                } else {
                    context.write('1_inventory', context.value);
                }

            } catch (e) {
                log.error('map Error', 'Key: ' + context.key + ', Error: ' + e.toString());
            }
        }

        /**
         * REDUCE - Create items and ensure locations exist
         */
        function reduce(context) {
            try {
                const stage = context.key;
                const records = context.values.map(v => JSON.parse(v));

                log.audit('========================================', '');
                log.audit('REDUCE - ' + stage, 'Processing ' + records.length + ' records');
                log.audit('========================================', '');

                let created = 0;
                let skipped = 0;
                let failed = 0;
                let locationsCreated = 0;
                let mrpUpdatesCount = 0;
                let locationMRPUpdated = 0;
                let vendorSubsidiaryUpdated = 0;

                records.forEach(rowData => {
                    try {
                        const externalId = rowData.prospectName + '_' + rowData.itemFields.itemid;

                        // Check if item already exists
                        const existingItemId = findItemByExternalId(externalId);

                        if (existingItemId) {
                            log.debug('Item Exists', 'Item: ' + rowData.itemFields.itemid + ' (ID: ' + existingItemId + ')');
                            skipped++;

                            // IDEMPOTENT: Check and create missing locations for existing item
                            const locsAdded = ensureItemLocations(existingItemId, rowData);
                            locationsCreated += locsAdded;

                            // IDEMPOTENT: Check and update MRP settings on existing item
                            const mrpUpdated = ensureMRPSettings(existingItemId, rowData);
                            if (mrpUpdated) {
                                mrpUpdatesCount++;
                            }

                            // IDEMPOTENT: Check and update MRP settings on existing item locations
                            locationMRPUpdated += ensureLocationMRPSettings(existingItemId, rowData);

                            // IDEMPOTENT: Check and update vendor subsidiary on existing item
                            if (ensureVendorSubsidiary(existingItemId, rowData.recordType, rowData)) {
                                vendorSubsidiaryUpdated++;
                            }

                        } else {
                            // Create new item
                            const itemId = createItem(rowData);

                            if (itemId) {
                                log.audit('Item Created', rowData.recordType + ': ' + rowData.itemFields.itemid + ' (ID: ' + itemId + ')');
                                created++;
                            } else {
                                failed++;
                            }
                        }
                    } catch (e) {
                        failed++;
                        log.error('Item Failed', 'Row ' + rowData.rowNumber + ' (' + rowData.itemFields.itemid + '): ' + e.toString());
                    }
                });

                log.audit('REDUCE Complete', stage + ' - Created: ' + created + ', Skipped: ' + skipped + ', Failed: ' + failed + ', Locations Added: ' + locationsCreated + ', MRP Updated: ' + mrpUpdatesCount + ', Location MRP Updated: ' + locationMRPUpdated + ', Vendor Subsidiary Updated: ' + vendorSubsidiaryUpdated);

            } catch (e) {
                log.error('reduce Error', 'Stage: ' + context.key + ', Error: ' + e.toString() + '\n' + e.stack);
            }
        }

        /**
         * Find item by external ID
         */
        function findItemByExternalId(externalId) {
            try {
                const itemSearch = search.create({
                    type: 'item',
                    filters: [['externalid', 'is', externalId]],
                    columns: ['internalid']
                });

                const results = itemSearch.run().getRange({ start: 0, end: 1 });
                
                if (results.length > 0) {
                    return results[0].getValue('internalid');
                }
            } catch (e) {
                log.debug('Item Search Error', e.toString());
            }
            return null;
        }

        /**
         * Create Item (Inventory or Assembly)
         */
        function createItem(rowData) {
            const defaults = rowData.defaults || DEFAULT_CONFIG;
            
            const itemRec = record.create({
                type: rowData.recordType,
                isDynamic: true
            });

            // Set External ID
            const externalId = rowData.prospectName + '_' + rowData.itemFields.itemid;
            itemRec.setValue({ fieldId: 'externalid', value: externalId });

            // Set mapped fields
            Object.keys(rowData.itemFields).forEach(field => {
                try {
                    const value = rowData.itemFields[field];
                    if (value) {
                        itemRec.setValue({ fieldId: field, value: value });
                    }
                } catch (e) {
                    log.debug('Field Set Warning', 'Field: ' + field + ', Error: ' + e.toString());
                }
            });

            // Set standard defaults
            itemRec.setValue({ fieldId: 'includechildren', value: true });
            itemRec.setValue({ fieldId: 'taxschedule', value: defaults.taxScheduleId });
            
            // Units Type = Each (ID: 1)
            try {
                itemRec.setValue({ fieldId: 'unitstype', value: 1 });
            } catch (e) {
                log.debug('Units Type Warning', e.toString());
            }

            // Weight = 1
            try {
                itemRec.setValue({ fieldId: 'weight', value: 1 });
            } catch (e) {
                log.debug('Weight Warning', e.toString());
            }

            // MRP Setup (if enabled)
            if (defaults.setupMRP) {
                // Set Planning Item Category
                if (rowData.planningItemCategoryId) {
                    try {
                        itemRec.setValue({
                            fieldId: 'planningitemcategory',
                            value: rowData.planningItemCategoryId
                        });
                        log.debug('Planning Item Category Set', rowData.planningItemCategoryId);
                    } catch (e) {
                        log.debug('Planning Item Category Warning', e.toString());
                    }
                }

                // Set Replenishment Method
                // 'MPS' = Master Production Scheduling (assemblies)
                // 'MRP' = Material Requirements Planning (inventory)
                try {
                    const replenishmentMethod = rowData.isAssembly ? 'MPS' : 'MRP';
                    itemRec.setValue({
                        fieldId: 'supplyreplenishmentmethod',
                        value: replenishmentMethod
                    });
                    log.debug('Replenishment Method Set', replenishmentMethod + ' for ' + rowData.recordType);
                } catch (e) {
                    log.debug('Replenishment Method Warning', e.toString());
                }
            }

            // Add vendor (inventory items only)
            const vendorId = getVendorForItem(rowData, defaults);
            if (vendorId && rowData.recordType === 'inventoryitem') {
                try {
                    itemRec.selectNewLine({ sublistId: 'itemvendor' });
                    itemRec.setCurrentSublistValue({
                        sublistId: 'itemvendor',
                        fieldId: 'vendor',
                        value: vendorId
                    });
                    itemRec.setCurrentSublistValue({
                        sublistId: 'itemvendor',
                        fieldId: 'preferredvendor',
                        value: true
                    });
                    itemRec.setCurrentSublistValue({
                        sublistId: 'itemvendor',
                        fieldId: 'purchaseprice',
                        value: defaults.purchasePrice
                    });
                    itemRec.setCurrentSublistValue({
                        sublistId: 'itemvendor',
                        fieldId: 'subsidiary',
                        value: defaults.vendorSubsidiaryId || defaults.subsidiaryId
                    });

                    if (rowData.itemFields.vendorpartnumber) {
                        itemRec.setCurrentSublistValue({
                            sublistId: 'itemvendor',
                            fieldId: 'vendorcode',
                            value: rowData.itemFields.vendorpartnumber
                        });
                    }
                    
                    itemRec.commitLine({ sublistId: 'itemvendor' });
                } catch (e) {
                    log.debug('Vendor Warning', e.toString());
                }
            }

            const itemId = itemRec.save();

            // Create item locations
            createItemLocations(itemId, rowData);

            return itemId;
        }

        /**
         * Get vendor ID for item
         */
        function getVendorForItem(rowData, defaults) {
            if (defaults.createVendors && rowData.vendorName && rowData.vendorCache) {
                const vendorId = rowData.vendorCache[rowData.vendorName];
                if (vendorId) {
                    return vendorId;
                }
            }
            
            if (defaults.vendorId > 0) {
                return defaults.vendorId;
            }

            return null;
        }

        /**
         * Create Item Locations using itemlocationconfiguration records
         */
        function createItemLocations(itemId, rowData) {
            const defaults = rowData.defaults || DEFAULT_CONFIG;
            const locationIds = defaults.locationIds || [];
            const isAssembly = rowData.isAssembly || false;
            const rotationIndex = rowData.mrpRotationIndex || 0;

            locationIds.forEach((locationId, locIndex) => {
                try {
                    // Use itemlocationconfiguration record to add location
                    const itemLocConfigRec = record.create({
                        type: 'itemlocationconfiguration',
                        defaultValues: {
                            item: itemId
                        }
                    });

                    // Set subsidiary (required)
                    itemLocConfigRec.setValue({
                        fieldId: 'subsidiary',
                        value: defaults.subsidiaryId || DEFAULT_CONFIG.subsidiaryId
                    });

                    // Set location
                    itemLocConfigRec.setValue({ fieldId: 'location', value: locationId });

                    // Set basic location defaults
                    const locDefaults = defaults.itemLocationDefaults || {};
                    itemLocConfigRec.setValue({ fieldId: 'preferredstocklevel', value: locDefaults.preferredstocklevel || 1000 });
                    itemLocConfigRec.setValue({ fieldId: 'reorderpoint', value: locDefaults.reorderpoint || 600 });
                    itemLocConfigRec.setValue({ fieldId: 'safetystocklevel', value: locDefaults.safetystocklevel || 100 });

                    // ========== MRP SETTINGS ==========
                    if (defaults.setupMRP) {
                        // Supply Type: BUILD for assemblies, PURCHASE for inventory
                        const supplyType = isAssembly ? 'BUILD' : 'PURCHASE';
                        itemLocConfigRec.setValue({ fieldId: 'supplytype', value: supplyType });

                        // Lead Time: Rotate through values
                        const leadTimeIndex = (rotationIndex + locIndex) % MRP_LOCATION_ROTATION.leadTimes.length;
                        const leadTime = MRP_LOCATION_ROTATION.leadTimes[leadTimeIndex];
                        itemLocConfigRec.setValue({ fieldId: 'leadtime', value: leadTime });

                        // Lot Sizing Method: Rotate through methods
                        const lotSizingIndex = rotationIndex % MRP_LOCATION_ROTATION.lotSizingMethods.length;
                        const lotSizingMethod = MRP_LOCATION_ROTATION.lotSizingMethods[lotSizingIndex];
                        itemLocConfigRec.setValue({ fieldId: 'supplylotsizingmethod', value: lotSizingMethod });

                        // Set parameters based on lot sizing method
                        if (lotSizingMethod === 'FIXED_LOT_SIZE') {
                            itemLocConfigRec.setValue({ fieldId: 'fixedlotsize', value: MRP_LOCATION_ROTATION.fixedLotSize });
                        } else if (lotSizingMethod === 'FIXED_LOT_MULTIPLE') {
                            itemLocConfigRec.setValue({ fieldId: 'fixedlotmultiple', value: MRP_LOCATION_ROTATION.fixedLotMultiple });
                        } else if (lotSizingMethod === 'PERIODIC_LOT_SIZE') {
                            itemLocConfigRec.setValue({ fieldId: 'periodiclotsizedays', value: MRP_LOCATION_ROTATION.periodicLotSizeDays });
                            itemLocConfigRec.setValue({ fieldId: 'periodiclotsizetype', value: MRP_LOCATION_ROTATION.periodicLotSizeType });
                        }

                        const configId = itemLocConfigRec.save();

                        log.debug('Item Location Created',
                            'Item: ' + itemId + ', Location: ' + locationId +
                            ', SupplyType: ' + supplyType + ', LotSizing: ' + lotSizingMethod +
                            ', LeadTime: ' + leadTime + ', Config ID: ' + configId);
                    } else {
                        // No MRP - use UI lead time
                        itemLocConfigRec.setValue({ fieldId: 'leadtime', value: locDefaults.leadtime || 7 });

                        const configId = itemLocConfigRec.save();

                        log.debug('Item Location Created', 'Item: ' + itemId + ', Location: ' + locationId + ', Config ID: ' + configId);
                    }

                } catch (e) {
                    log.error('Item Location Error', 'Item: ' + itemId + ', Location: ' + locationId + ', Error: ' + e.toString());
                }
            });
        }

        /**
         * IDEMPOTENT: Ensure locations exist for an existing item
         * Uses itemlocationconfiguration records (not sublist) for existing items
         * Returns number of locations added
         */
        function ensureItemLocations(itemId, rowData) {
            const defaults = rowData.defaults || DEFAULT_CONFIG;
            const locationIds = defaults.locationIds || [];
            let locationsAdded = 0;

            // Get existing locations for this item
            const existingLocations = getExistingItemLocations(itemId);
            
            locationIds.forEach(locationId => {
                if (existingLocations.includes(parseInt(locationId))) {
                    log.debug('Location Exists', 'Item: ' + itemId + ', Location: ' + locationId);
                    return;
                }

                try {
                    // Use itemlocationconfiguration record to add location to existing item
                    const itemLocConfigRec = record.create({
                        type: 'itemlocationconfiguration',
                        defaultValues: {
                            item: itemId
                        }
                    });

                    // Set subsidiary (required)
                    itemLocConfigRec.setValue({ 
                        fieldId: 'subsidiary', 
                        value: defaults.subsidiaryId || DEFAULT_CONFIG.subsidiaryId 
                    });

                    // Set location
                    itemLocConfigRec.setValue({ fieldId: 'location', value: locationId });

                    // Set location-specific inventory settings
                    const locDefaults = defaults.itemLocationDefaults || {};
                    itemLocConfigRec.setValue({ 
                        fieldId: 'preferredstocklevel', 
                        value: locDefaults.preferredstocklevel || 1000 
                    });
                    itemLocConfigRec.setValue({ 
                        fieldId: 'reorderpoint', 
                        value: locDefaults.reorderpoint || 600 
                    });
                    itemLocConfigRec.setValue({ 
                        fieldId: 'safetystocklevel', 
                        value: locDefaults.safetystocklevel || 100 
                    });
                    itemLocConfigRec.setValue({ 
                        fieldId: 'leadtime', 
                        value: locDefaults.leadtime || 7 
                    });

                    const configId = itemLocConfigRec.save();

                    log.audit('Location Added to Existing Item', 'Item: ' + itemId + ', Location: ' + locationId + ', Config ID: ' + configId);
                    locationsAdded++;

                } catch (e) {
                    log.error('Add Location Error', 'Item: ' + itemId + ', Location: ' + locationId + ', Error: ' + e.toString());
                }
            });

            return locationsAdded;
        }

        /**
         * IDEMPOTENT: Ensure MRP settings are configured on existing items
         * Returns true if any updates were made, false if already configured
         */
        function ensureMRPSettings(itemId, rowData) {
            const defaults = rowData.defaults || DEFAULT_CONFIG;

            // Skip if MRP setup is disabled
            if (!defaults.setupMRP) {
                return false;
            }

            let needsSave = false;

            try {
                const itemRec = record.load({
                    type: rowData.recordType,
                    id: itemId,
                    isDynamic: true
                });

                // Check/set Planning Item Category
                if (rowData.planningItemCategoryId) {
                    const currentCategory = itemRec.getValue({ fieldId: 'planningitemcategory' });
                    if (!currentCategory) {
                        try {
                            itemRec.setValue({
                                fieldId: 'planningitemcategory',
                                value: rowData.planningItemCategoryId
                            });
                            needsSave = true;
                            log.debug('Planning Item Category Updated', 'Item: ' + itemId + ', Category: ' + rowData.planningItemCategoryId);
                        } catch (e) {
                            log.debug('Planning Item Category Update Warning', 'Item: ' + itemId + ', Error: ' + e.toString());
                        }
                    }
                }

                // Check/set Replenishment Method
                // 'MPS' = Master Production Scheduling (assemblies)
                // 'MRP' = Material Requirements Planning (inventory)
                const expectedMethod = rowData.isAssembly ? 'MPS' : 'MRP';
                const currentMethod = itemRec.getValue({ fieldId: 'supplyreplenishmentmethod' });

                // Check if not set or set to wrong value ('REORDER_POINT' is the default)
                if (!currentMethod || currentMethod === 'REORDER_POINT' || currentMethod !== expectedMethod) {
                    try {
                        itemRec.setValue({
                            fieldId: 'supplyreplenishmentmethod',
                            value: expectedMethod
                        });
                        needsSave = true;
                        log.debug('Replenishment Method Updated', 'Item: ' + itemId + ', Method: ' + expectedMethod + ' (was: ' + currentMethod + ')');
                    } catch (e) {
                        log.debug('Replenishment Method Update Warning', 'Item: ' + itemId + ', Error: ' + e.toString());
                    }
                }

                // Save if any changes were made
                if (needsSave) {
                    itemRec.save();
                    log.audit('MRP Settings Updated', 'Item: ' + itemId);
                    return true;
                } else {
                    log.debug('MRP Settings Already Configured', 'Item: ' + itemId);
                    return false;
                }

            } catch (e) {
                log.error('ensureMRPSettings Error', 'Item: ' + itemId + ', Error: ' + e.toString());
                return false;
            }
        }

        /**
         * IDEMPOTENT: Ensure MRP settings are configured on existing item location records
         * Returns number of locations updated
         */
        function ensureLocationMRPSettings(itemId, rowData) {
            const defaults = rowData.defaults || DEFAULT_CONFIG;

            // Skip if MRP setup is disabled
            if (!defaults.setupMRP) {
                return 0;
            }

            const isAssembly = rowData.isAssembly || false;
            const rotationIndex = rowData.mrpRotationIndex || 0;
            let locationsUpdated = 0;

            try {
                const locConfigSearch = search.create({
                    type: 'itemlocationconfiguration',
                    filters: [['item', 'anyof', itemId]],
                    columns: ['internalid', 'location', 'supplytype', 'supplylotsizingmethod', 'leadtime']
                });

                const results = [];
                locConfigSearch.run().each(function(result) {
                    results.push({
                        configId: result.getValue('internalid'),
                        locationId: result.getValue('location'),
                        currentSupplyType: result.getValue('supplytype'),
                        currentLotSizing: result.getValue('supplylotsizingmethod'),
                        currentLeadTime: result.getValue('leadtime')
                    });
                    return true;
                });

                results.forEach((locConfig, locIndex) => {
                    try {
                        const expectedSupplyType = isAssembly ? 'BUILD' : 'PURCHASE';
                        const leadTimeIndex = (rotationIndex + locIndex) % MRP_LOCATION_ROTATION.leadTimes.length;
                        const expectedLeadTime = MRP_LOCATION_ROTATION.leadTimes[leadTimeIndex];
                        const lotSizingIndex = rotationIndex % MRP_LOCATION_ROTATION.lotSizingMethods.length;
                        const expectedLotSizing = MRP_LOCATION_ROTATION.lotSizingMethods[lotSizingIndex];

                        const needsUpdate =
                            locConfig.currentSupplyType !== expectedSupplyType ||
                            !locConfig.currentLotSizing ||
                            locConfig.currentLotSizing !== expectedLotSizing ||
                            !locConfig.currentLeadTime ||
                            parseInt(locConfig.currentLeadTime) !== expectedLeadTime;

                        if (!needsUpdate) {
                            log.debug('Location MRP Already Configured', 'Item: ' + itemId + ', Location: ' + locConfig.locationId);
                            return;
                        }

                        const configRec = record.load({
                            type: 'itemlocationconfiguration',
                            id: locConfig.configId,
                            isDynamic: true
                        });

                        configRec.setValue({ fieldId: 'supplytype', value: expectedSupplyType });
                        configRec.setValue({ fieldId: 'leadtime', value: expectedLeadTime });
                        configRec.setValue({ fieldId: 'supplylotsizingmethod', value: expectedLotSizing });

                        if (expectedLotSizing === 'FIXED_LOT_SIZE') {
                            configRec.setValue({ fieldId: 'fixedlotsize', value: MRP_LOCATION_ROTATION.fixedLotSize });
                        } else if (expectedLotSizing === 'FIXED_LOT_MULTIPLE') {
                            configRec.setValue({ fieldId: 'fixedlotmultiple', value: MRP_LOCATION_ROTATION.fixedLotMultiple });
                        } else if (expectedLotSizing === 'PERIODIC_LOT_SIZE') {
                            configRec.setValue({ fieldId: 'periodiclotsizedays', value: MRP_LOCATION_ROTATION.periodicLotSizeDays });
                            configRec.setValue({ fieldId: 'periodiclotsizetype', value: MRP_LOCATION_ROTATION.periodicLotSizeType });
                        }

                        configRec.save();
                        locationsUpdated++;

                        log.audit('Location MRP Updated',
                            'Item: ' + itemId + ', Location: ' + locConfig.locationId +
                            ', SupplyType: ' + expectedSupplyType + ', LotSizing: ' + expectedLotSizing +
                            ', LeadTime: ' + expectedLeadTime);

                    } catch (e) {
                        log.error('Location MRP Update Error', 'Item: ' + itemId + ', Config: ' + locConfig.configId + ', Error: ' + e.toString());
                    }
                });

            } catch (e) {
                log.error('ensureLocationMRPSettings Error', 'Item: ' + itemId + ', Error: ' + e.toString());
            }

            return locationsUpdated;
        }

        /**
         * IDEMPOTENT: Ensure vendor subsidiary is correct on existing items
         * Returns true if any updates were made
         */
        function ensureVendorSubsidiary(itemId, recordType, rowData) {
            const defaults = rowData.defaults || DEFAULT_CONFIG;
            const expectedSubsidiary = defaults.vendorSubsidiaryId || defaults.subsidiaryId;

            if (!expectedSubsidiary) {
                return false;
            }

            try {
                const itemRec = record.load({
                    type: recordType,
                    id: itemId,
                    isDynamic: true
                });

                const lineCount = itemRec.getLineCount({ sublistId: 'itemvendor' });

                if (lineCount === 0) {
                    return false;
                }

                let updated = false;

                for (let i = 0; i < lineCount; i++) {
                    const currentSubsidiary = itemRec.getSublistValue({
                        sublistId: 'itemvendor',
                        fieldId: 'subsidiary',
                        line: i
                    });

                    if (currentSubsidiary != expectedSubsidiary) {
                        const vendorName = itemRec.getSublistText({
                            sublistId: 'itemvendor',
                            fieldId: 'vendor',
                            line: i
                        });

                        itemRec.selectLine({ sublistId: 'itemvendor', line: i });
                        itemRec.setCurrentSublistValue({
                            sublistId: 'itemvendor',
                            fieldId: 'subsidiary',
                            value: expectedSubsidiary
                        });
                        itemRec.commitLine({ sublistId: 'itemvendor' });
                        updated = true;

                        log.audit('Vendor Subsidiary Updated',
                            'Item: ' + itemId + ', Vendor: ' + vendorName +
                            ', Old Subsidiary: ' + currentSubsidiary +
                            ', New Subsidiary: ' + expectedSubsidiary);
                    }
                }

                if (updated) {
                    itemRec.save();
                }

                return updated;

            } catch (e) {
                log.error('ensureVendorSubsidiary Error', 'Item: ' + itemId + ', Error: ' + e.toString());
                return false;
            }
        }

        /**
         * Get list of existing location IDs for an item by searching itemlocationconfiguration records
         */
        function getExistingItemLocations(itemId) {
            const locations = [];
            
            try {
                // Search itemlocationconfiguration records for this item
                const locConfigSearch = search.create({
                    type: 'itemlocationconfiguration',
                    filters: [['item', 'anyof', itemId]],
                    columns: ['location']
                });

                locConfigSearch.run().each(function(result) {
                    const locId = result.getValue('location');
                    if (locId) {
                        locations.push(parseInt(locId));
                    }
                    return true;
                });

                log.debug('Existing Locations Found', 'Item: ' + itemId + ', Locations: ' + JSON.stringify(locations));

            } catch (e) {
                log.debug('Get Existing Locations Error', e.toString());
            }

            return locations;
        }

        /**
         * SUMMARIZE - Create BOMs, BOM Revisions, and link to assemblies
         */
        function summarize(context) {
            try {
                log.audit('========================================', '');
                log.audit('SUMMARIZE - Creating BOMs', '');
                log.audit('========================================', '');

                const scriptObj = runtime.getCurrentScript();
                const configFileId = scriptObj.getParameter({ name: 'custscript_bom_config_file_id' });
                const configFile = file.load({ id: configFileId });
                const config = JSON.parse(configFile.getContents());
                const prospectName = config.prospectName;
                const defaults = config.defaults || DEFAULT_CONFIG;

                log.audit('Config Reloaded', 'Prospect: ' + prospectName);

                // Get rows from cache or re-parse
                let allRows;
                try {
                    const importCache = cache.getCache({
                        name: 'BOM_IMPORT_CACHE',
                        scope: cache.Scope.PRIVATE
                    });
                    const rowsStr = importCache.get({ key: 'ALL_ROWS' });
                    allRows = JSON.parse(rowsStr);
                    log.audit('Rows from Cache', allRows.length + ' rows');
                } catch (e) {
                    log.audit('Cache Miss', 'Re-parsing CSV file');
                    allRows = reParseCSVForSummarize(config);
                }

                // Find all assemblies
                const assemblies = allRows.filter(row => row.isAssembly);
                
                log.audit('Assemblies to Process', assemblies.length);

                let bomsCreated = 0;
                let bomsFailed = 0;
                let revisionsCreated = 0;
                let revisionsFailed = 0;
                let linksCreated = 0;

                assemblies.forEach(assembly => {
                    try {
                        const assemblyItemId = assembly.itemFields.itemid;
                        const assemblyHierarchy = assembly.hierarchy;

                        // Get assembly internal ID
                        const assemblyExternalId = prospectName + '_' + assemblyItemId;
                        const assemblyInternalId = findItemByExternalId(assemblyExternalId);

                        if (!assemblyInternalId) {
                            log.error('Assembly Not Found', 'Assembly ' + assemblyItemId + ' not found');
                            bomsFailed++;
                            return;
                        }

                        // Find direct children
                        const directChildren = allRows.filter(row => row.parentHierarchy === assemblyHierarchy);

                        if (directChildren.length === 0) {
                            log.audit('No Children', 'Assembly ' + assemblyItemId + ' has no direct children');
                            return;
                        }

                        // Build component list
                        const components = [];
                        directChildren.forEach(child => {
                            // Debug: Log component data to troubleshoot quantity issues
                            log.debug('Component Data', 'Item: ' + child.itemFields.itemid +
                                ', bomFields: ' + JSON.stringify(child.bomFields) +
                                ', quantity: ' + child.bomFields.quantity);

                            const childExternalId = prospectName + '_' + child.itemFields.itemid;
                            const childInternalId = findItemByExternalId(childExternalId);

                            if (childInternalId) {
                                components.push({
                                    itemId: child.itemFields.itemid,
                                    internalId: childInternalId,
                                    quantity: child.bomFields.quantity || 1
                                });
                            } else {
                                log.error('Component Not Found', 'Component ' + child.itemFields.itemid + ' not found');
                            }
                        });

                        if (components.length === 0) {
                            log.error('No Valid Components', 'Assembly ' + assemblyItemId + ' has no valid components');
                            bomsFailed++;
                            return;
                        }

                        log.audit('Creating BOM', 'Assembly: ' + assemblyItemId + ' with ' + components.length + ' components');

                        // Create or get BOM
                        const bomResult = createOrGetBOM(assemblyItemId, assemblyInternalId, prospectName, defaults);

                        if (bomResult.bomId) {
                            if (bomResult.created) {
                                bomsCreated++;
                            }

                            // Create or get BOM Revision
                            const revisionResult = createOrGetBOMRevision(bomResult.bomId, assemblyItemId, components, prospectName);
                            
                            if (revisionResult.created) {
                                revisionsCreated++;
                            } else if (revisionResult.exists) {
                                // Revision already exists
                            } else {
                                revisionsFailed++;
                            }

                            // IDEMPOTENT: Link BOM to assembly item if not already linked
                            const linked = ensureBOMLinkedToAssembly(assemblyInternalId, bomResult.bomId, assemblyItemId);
                            if (linked) {
                                linksCreated++;
                            }

                        } else {
                            bomsFailed++;
                        }

                    } catch (e) {
                        bomsFailed++;
                        log.error('BOM Process Failed', 'Assembly: ' + assembly.itemFields.itemid + ', Error: ' + e.toString());
                    }
                });

                // Log summary
                log.audit('========================================', '');
                log.audit('IMPORT SUMMARY', '');
                log.audit('========================================', '');
                log.audit('Prospect', prospectName);
                log.audit('BOMs Created', bomsCreated);
                log.audit('BOMs Failed', bomsFailed);
                log.audit('Revisions Created', revisionsCreated);
                log.audit('Revisions Failed', revisionsFailed);
                log.audit('BOM Links Created', linksCreated);

                logStageErrors(context);

                log.audit('========================================', '');
                log.audit('IMPORT COMPLETE', '');
                log.audit('========================================', '');

            } catch (e) {
                log.error('summarize Error', e.toString() + '\n' + e.stack);
            }
        }

        /**
         * Create or get existing BOM
         */
        function createOrGetBOM(assemblyItemId, assemblyInternalId, prospectName, defaults) {
            try {
                const bomName = assemblyItemId + '_BOM';
                const externalId = prospectName + '_' + bomName;

                // Check if BOM already exists
                const existingBom = findBOMByExternalId(externalId);
                if (existingBom) {
                    log.debug('BOM Exists', 'BOM: ' + bomName + ' (ID: ' + existingBom + ')');
                    return { bomId: existingBom, created: false };
                }

                const bomRec = record.create({
                    type: 'bom',
                    isDynamic: true
                });

                bomRec.setValue({ fieldId: 'name', value: bomName });
                bomRec.setValue({ fieldId: 'subsidiary', value: defaults.subsidiaryId || DEFAULT_CONFIG.subsidiaryId });
                bomRec.setValue({ fieldId: 'includechildren', value: true });
                bomRec.setValue({ fieldId: 'externalid', value: externalId });

                const bomId = bomRec.save();

                log.audit('BOM Created', 'BOM: ' + bomName + ' (ID: ' + bomId + ')');

                return { bomId: bomId, created: true };

            } catch (e) {
                log.error('BOM Creation Failed', 'Assembly: ' + assemblyItemId + ', Error: ' + e.toString());
                return { bomId: null, created: false };
            }
        }

        /**
         * Find BOM by external ID
         */
        function findBOMByExternalId(externalId) {
            try {
                const bomSearch = search.create({
                    type: 'bom',
                    filters: [['externalid', 'is', externalId]],
                    columns: ['internalid']
                });

                const results = bomSearch.run().getRange({ start: 0, end: 1 });
                
                if (results.length > 0) {
                    return results[0].getValue('internalid');
                }
            } catch (e) {
                log.debug('BOM Search Error', e.toString());
            }
            return null;
        }

        /**
         * Create or get existing BOM Revision with components
         */
        function createOrGetBOMRevision(bomId, assemblyItemId, components, prospectName) {
            try {
                const revisionName = assemblyItemId + '_REV_A';
                const externalId = prospectName + '_' + revisionName;

                // Check if revision already exists
                const existingRevision = findBOMRevisionByExternalId(externalId);
                if (existingRevision) {
                    log.debug('BOM Revision Exists', 'Revision: ' + revisionName + ' (ID: ' + existingRevision + ')');
                    return { revisionId: existingRevision, created: false, exists: true };
                }

                const yesterday = new Date();
                yesterday.setDate(yesterday.getDate() - 1);

                const bomRevRec = record.create({
                    type: 'bomrevision',
                    isDynamic: true
                });

                bomRevRec.setValue({ fieldId: 'name', value: revisionName });
                bomRevRec.setValue({ fieldId: 'billofmaterials', value: bomId });
                bomRevRec.setValue({ fieldId: 'effectivestartdate', value: yesterday });
                bomRevRec.setValue({ fieldId: 'memo', value: 'Initial revision - ' + prospectName });
                bomRevRec.setValue({ fieldId: 'externalid', value: externalId });

                // Add components
                let componentsAdded = 0;
                components.forEach(component => {
                    try {
                        bomRevRec.selectNewLine({ sublistId: 'component' });
                        bomRevRec.setCurrentSublistValue({
                            sublistId: 'component',
                            fieldId: 'item',
                            value: component.internalId
                        });
                        bomRevRec.setCurrentSublistValue({
                            sublistId: 'component',
                            fieldId: 'bomquantity',
                            value: component.quantity
                        });
                        bomRevRec.commitLine({ sublistId: 'component' });
                        componentsAdded++;
                    } catch (e) {
                        log.error('Component Add Failed', 'Component: ' + component.itemId + ', Error: ' + e.toString());
                    }
                });

                const revisionId = bomRevRec.save();

                log.audit('BOM Revision Created', 'Revision: ' + revisionName + ' (ID: ' + revisionId + ') with ' + componentsAdded + ' components');

                return { revisionId: revisionId, created: true, exists: false };

            } catch (e) {
                log.error('BOM Revision Failed', 'Assembly: ' + assemblyItemId + ', Error: ' + e.toString());
                return { revisionId: null, created: false, exists: false };
            }
        }

        /**
         * Find BOM Revision by external ID
         */
        function findBOMRevisionByExternalId(externalId) {
            try {
                const revSearch = search.create({
                    type: 'bomrevision',
                    filters: [['externalid', 'is', externalId]],
                    columns: ['internalid']
                });

                const results = revSearch.run().getRange({ start: 0, end: 1 });
                
                if (results.length > 0) {
                    return results[0].getValue('internalid');
                }
            } catch (e) {
                log.debug('BOM Revision Search Error', e.toString());
            }
            return null;
        }

        /**
         * IDEMPOTENT: Ensure BOM is linked to assembly item via billofmaterials sublist
         * Also ensures masterdefault is set on the BOM link
         * Returns true if link was created or updated, false if already complete
         */
        function ensureBOMLinkedToAssembly(assemblyInternalId, bomId, assemblyItemId) {
            try {
                // Load assembly to check if BOM is already linked
                const assemblyRec = record.load({
                    type: 'assemblyitem',
                    id: assemblyInternalId,
                    isDynamic: true
                });

                // Check existing BOM links on the billofmaterials sublist
                const lineCount = assemblyRec.getLineCount({ sublistId: 'billofmaterials' });
                
                for (let i = 0; i < lineCount; i++) {
                    const linkedBomId = assemblyRec.getSublistValue({
                        sublistId: 'billofmaterials',
                        fieldId: 'billofmaterials',
                        line: i
                    });
                    
                    if (linkedBomId == bomId) {
                        // BOM is already linked - check if masterdefault is set
                        const isMasterDefault = assemblyRec.getSublistValue({
                            sublistId: 'billofmaterials',
                            fieldId: 'masterdefault',
                            line: i
                        });
                        
                        if (isMasterDefault) {
                            log.debug('BOM Already Linked with Master Default', 'Assembly: ' + assemblyItemId + ', BOM ID: ' + bomId);
                            return false;
                        }
                        
                        // Update existing line to set masterdefault
                        assemblyRec.selectLine({ sublistId: 'billofmaterials', line: i });
                        assemblyRec.setCurrentSublistValue({
                            sublistId: 'billofmaterials',
                            fieldId: 'masterdefault',
                            value: true
                        });
                        assemblyRec.commitLine({ sublistId: 'billofmaterials' });
                        assemblyRec.save();
                        
                        log.audit('BOM Master Default Set', 'Assembly: ' + assemblyItemId + ' (ID: ' + assemblyInternalId + '), BOM ID: ' + bomId);
                        return true;
                    }
                }

                // BOM not linked yet - add it
                assemblyRec.selectNewLine({ sublistId: 'billofmaterials' });
                assemblyRec.setCurrentSublistValue({
                    sublistId: 'billofmaterials',
                    fieldId: 'billofmaterials',
                    value: bomId
                });
                // Set as Master Default
                assemblyRec.setCurrentSublistValue({
                    sublistId: 'billofmaterials',
                    fieldId: 'masterdefault',
                    value: true
                });
                assemblyRec.commitLine({ sublistId: 'billofmaterials' });

                assemblyRec.save();

                log.audit('BOM Linked to Assembly', 'Assembly: ' + assemblyItemId + ' (ID: ' + assemblyInternalId + '), BOM ID: ' + bomId + ', Master Default: true');
                return true;

            } catch (e) {
                log.error('BOM Link Failed', 'Assembly: ' + assemblyItemId + ', BOM ID: ' + bomId + ', Error: ' + e.toString());
                return false;
            }
        }

        /**
         * Re-parse CSV for summarize stage if cache missed
         */
        function reParseCSVForSummarize(config) {
            const csvFile = file.load({ id: config.csvFileId });
            const csvContent = csvFile.getContents();
            const lines = csvContent.split('\n').filter(line => line.trim());

            const allRows = [];
            for (let i = 1; i < lines.length; i++) {
                const rowData = parseRow(lines[i]);
                if (rowData.some(cell => cell.trim())) {
                    allRows.push({
                        rowNumber: i + 1,
                        cells: rowData
                    });
                }
            }

            const mappedRows = allRows.map(row => {
                const mapped = {
                    rowNumber: row.rowNumber,
                    hierarchy: null,
                    itemFields: {},
                    bomFields: {}
                };

                Object.keys(config.mappings).forEach(colIndex => {
                    const fieldName = config.mappings[colIndex];
                    const value = row.cells[parseInt(colIndex)] || '';

                    if (!value.trim()) return;

                    if (fieldName === 'hierarchy') {
                        mapped.hierarchy = value.trim();
                    } else if (fieldName === 'quantity') {
                        mapped.bomFields.quantity = parseFloat(value) || 1;
                    } else if (fieldName === 'itemid') {
                        mapped.itemFields.itemid = value.trim();
                    }
                });

                return mapped;
            }).filter(row => row.hierarchy && row.itemFields.itemid);

            // Determine assembly status
            const hierarchySet = new Set(mappedRows.map(r => r.hierarchy));
            
            mappedRows.forEach(row => {
                const isAssembly = Array.from(hierarchySet).some(h => 
                    h !== row.hierarchy && h.startsWith(row.hierarchy + '.')
                );
                row.isAssembly = isAssembly;

                const parts = row.hierarchy.split('.');
                if (parts.length > 1) {
                    parts.pop();
                    row.parentHierarchy = parts.join('.');
                } else {
                    row.parentHierarchy = null;
                }
            });

            return mappedRows;
        }

        /**
         * Log Map/Reduce stage errors
         */
        function logStageErrors(context) {
            if (context.inputSummary.error) {
                log.error('INPUT Error', context.inputSummary.error);
            }

            context.mapSummary.errors.iterator().each(function(key, error) {
                log.error('MAP Error', 'Key: ' + key + ', Error: ' + error);
                return true;
            });

            context.reduceSummary.errors.iterator().each(function(key, error) {
                log.error('REDUCE Error', 'Key: ' + key + ', Error: ' + error);
                return true;
            });
        }

        return {
            getInputData: getInputData,
            map: map,
            reduce: reduce,
            summarize: summarize
        };
    });