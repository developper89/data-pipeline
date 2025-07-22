# normalizer/validator.py - Optimized Version

import logging
import copy
import json
from typing import Any, Dict, List, Optional, Union, Tuple
from datetime import datetime

from shared.models.common import StandardizedOutput, ValidatedOutput, ErrorMessage
from preservarium_sdk.infrastructure.sql_repository.sql_datatype_repository import SQLDatatypeRepository
from preservarium_sdk.domain.model.datatype import DatatypeType

logger = logging.getLogger(__name__)

class Validator:
    """
    Validates and normalizes sensor data based on configurable rules stored in the datatypes table.
    Supports None values in constraint arrays where None indicates no constraint for that index.
    """
    
    def __init__(self, datatype_repository):
        """Initialize the validator with repository for accessing validation rules."""
        self.datatype_repository = datatype_repository
        
    async def validate_and_normalize(
        self, 
        device_id: str,
        standardized_data: StandardizedOutput,
        request_id: Optional[str] = None,
        datatype: Optional[Any] = None
    ) -> Tuple[Optional[StandardizedOutput], List[ErrorMessage]]:
        """
        Validate and normalize data based on datatype configuration.
        
        Args:
            device_id: The ID of the device sending the data
            standardized_data: StandardizedOutput object from the parser script
            request_id: Optional request ID for tracing
            datatype: Optional datatype object (if already fetched)
            
        Returns:
            Tuple with validated StandardizedOutput (or None if validation failed)
            and a list of any validation errors
        """
        errors = []
        
        # Clone datatype to avoid modifying the original
        datatype = copy.deepcopy(datatype)
        
        # Set datatype name
        datatype_name = (str(datatype.category) if len(list(dict.fromkeys(datatype.display_names))) > 1 
                        else datatype.display_names[0])
        datatype.name = datatype_name
        
        # Align datatype arrays with standardized_data and filter unmatched labels/values
        if standardized_data.labels and hasattr(datatype, 'labels') and datatype.labels:
            datatype, standardized_data = self._align_datatype_with_standardized_data(
                datatype, standardized_data, request_id
            )
        
        values = standardized_data.values
        
        # Process display names
        if standardized_data.display_names:
            processed_display_names = self._process_display_name_replacement(
                standardized_data.display_names, [datatype.name] * len(values)
            )
            datatype.display_names = processed_display_names
            logger.debug(f"Updated display names for datatype {datatype.id}: {processed_display_names}")
        
        # Auto-discovery if enabled
        if datatype and datatype.auto_discovery:
            datatype = await self._perform_auto_discovery(datatype, standardized_data, device_id)
        
        # Validation pipeline
        validation_results = self._run_validation_pipeline(values, datatype, standardized_data, device_id, request_id)
        
        # Process validation results
        validated_output, validation_errors = self._process_validation_results(
            validation_results, datatype, standardized_data, device_id, request_id
        )
        
        errors.extend(validation_errors)
        return validated_output, errors
    
    def _run_validation_pipeline(
        self, 
        values: List[Any], 
        datatype: Any, 
        standardized_data: StandardizedOutput,
        device_id: str,
        request_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Run the complete validation pipeline and return consolidated results."""
        
        # 1. Whitelist validation (skips other validations for whitelisted values)
        whitelist_valid, whitelist_results, whitelist_errors = self._validate_whitelist(
            values, datatype, standardized_data
        )
        
        # 2. Type validation (skipped for whitelisted values)
        expected_types = getattr(datatype, 'data_type', [])
        type_valid, converted_values, type_validation_results, type_errors = self._validate_type(
            values, expected_types, standardized_data, whitelist_results
        )
        
        # 3. Range validation (skipped for whitelisted values)
        range_valid, range_results, range_errors = self._validate_range(
            converted_values, datatype, standardized_data, whitelist_results
        )
        
        return {
            'whitelist_valid': whitelist_valid,
            'whitelist_results': whitelist_results,
            'whitelist_errors': whitelist_errors,
            'type_valid': type_valid,
            'converted_values': converted_values,
            'type_validation_results': type_validation_results,
            'type_errors': type_errors,
            'range_valid': range_valid,
            'range_results': range_results,
            'range_errors': range_errors
        }
    
    def _process_validation_results(
        self,
        validation_results: Dict[str, Any],
        datatype: Any,
        standardized_data: StandardizedOutput,
        device_id: str,
        request_id: Optional[str] = None
    ) -> Tuple[Optional[ValidatedOutput], List[ErrorMessage]]:
        """Process validation results and build validated output."""
        
        errors = []
        
        # Collect all validation errors
        all_errors = (validation_results['whitelist_errors'] + 
                     validation_results['type_errors'] + 
                     validation_results['range_errors'])
        
        for error_msg in all_errors:
            errors.append(ErrorMessage(
                error=error_msg,
                original_message={"device_id": device_id},
                request_id=request_id
            ))
        
        converted_values = validation_results['converted_values']
        whitelist_results = validation_results['whitelist_results']
        type_validation_results = validation_results['type_validation_results']
        range_results = validation_results['range_results']
        
        if not converted_values or not range_results:
            logger.warning(f"[{request_id}] No valid values found after validation for device {device_id}")
            return None, errors
        
        # Determine valid indices
        valid_indices = self._get_valid_indices(
            converted_values, whitelist_results, type_validation_results, range_results
        )
        
        if not valid_indices:
            logger.warning(f"[{request_id}] No valid values found after validation for device {device_id}")
            return None, errors
        
        # Build validated output with filtered data
        validated_output = self._build_validated_output_from_indices(
            valid_indices, converted_values, datatype, standardized_data, device_id, request_id
        )
        
        # Log partial validation warning if needed
        if len(valid_indices) < len(converted_values):
            logger.warning(f"[{request_id}] Partial validation: {len(valid_indices)} out of {len(converted_values)} values passed validation for device {device_id}")
        
        return validated_output, errors
    
    def _get_valid_indices(
        self,
        converted_values: List[Any],
        whitelist_results: List[bool],
        type_validation_results: List[bool],
        range_results: List[bool]
    ) -> List[int]:
        """Determine which value indices are valid based on all validation results."""
        valid_indices = []
        
        for i in range(len(converted_values)):
            # A value is valid if it's whitelisted OR passes both type and range validation
            whitelisted = i < len(whitelist_results) and whitelist_results[i]
            type_valid_for_index = i < len(type_validation_results) and type_validation_results[i]
            range_valid_for_index = i < len(range_results) and range_results[i]
            
            if whitelisted or (type_valid_for_index and range_valid_for_index):
                valid_indices.append(i)
        
        return valid_indices
    
    def _build_validated_output_from_indices(
        self,
        valid_indices: List[int],
        converted_values: List[Any],
        datatype: Any,
        standardized_data: StandardizedOutput,
        device_id: str,
        request_id: Optional[str] = None
    ) -> ValidatedOutput:
        """Build validated output using only the valid indices."""
        
        # Filter all arrays to only include valid indices
        filtered_data = self._filter_arrays_by_indices(
            valid_indices, converted_values, datatype, standardized_data
        )
        
        # Create filtered standardized_data object
        filtered_standardized_data = StandardizedOutput(
            device_id=standardized_data.device_id,
            values=filtered_data['values'],
            labels=filtered_data['std_labels'],
            display_names=filtered_data['std_display_names'],
            index=standardized_data.index,
            metadata=standardized_data.metadata,
            request_id=standardized_data.request_id,
            timestamp=standardized_data.timestamp
        )
        
        # Create filtered datatype object
        filtered_datatype = self._create_filtered_datatype(datatype, filtered_data)
        
        return self._build_validated_output(
            device_id, filtered_datatype, filtered_data['values'], request_id, filtered_standardized_data
        )
    
    def _filter_arrays_by_indices(
        self,
        valid_indices: List[int],
        converted_values: List[Any],
        datatype: Any,
        standardized_data: StandardizedOutput
    ) -> Dict[str, List[Any]]:
        """Filter all relevant arrays by valid indices."""
        
        def safe_filter(array: List[Any], indices: List[int]) -> List[Any]:
            """Safely filter array by indices."""
            return [array[i] for i in indices if i < len(array)] if array else []
        
        return {
            'values': safe_filter(converted_values, valid_indices),
            'std_labels': safe_filter(standardized_data.labels or [], valid_indices),
            'std_display_names': safe_filter(standardized_data.display_names or [], valid_indices),
            'dt_labels': safe_filter(getattr(datatype, 'labels', []), valid_indices),
            'dt_display_names': safe_filter(getattr(datatype, 'display_names', []), valid_indices),
            'dt_units': safe_filter(getattr(datatype, 'unit', []), valid_indices),
            'dt_persist': safe_filter(getattr(datatype, 'persist', []), valid_indices),
            'dt_min': safe_filter(getattr(datatype, 'min', []), valid_indices),
            'dt_max': safe_filter(getattr(datatype, 'max', []), valid_indices),
            'dt_data_type': safe_filter(getattr(datatype, 'data_type', []), valid_indices),
            'dt_possible_values': safe_filter(getattr(datatype, 'possible_values', []), valid_indices),
            'dt_whitelist_values': safe_filter(getattr(datatype, 'whitelist_values', []), valid_indices),
            'dt_read_only': safe_filter(getattr(datatype, 'read_only', []), valid_indices),
            'dt_description': safe_filter(getattr(datatype, 'description', []), valid_indices),
        }
    
    def _create_filtered_datatype(self, datatype: Any, filtered_data: Dict[str, List[Any]]) -> Any:
        """Create a filtered datatype object with only valid indices."""
        filtered_datatype = copy.deepcopy(datatype)
        
        # Map of attribute names to filtered data keys
        attr_mapping = {
            'labels': 'dt_labels',
            'display_names': 'dt_display_names',
            'unit': 'dt_units',
            'persist': 'dt_persist',
            'min': 'dt_min',
            'max': 'dt_max',
            'data_type': 'dt_data_type',
            'possible_values': 'dt_possible_values',
            'whitelist_values': 'dt_whitelist_values',
            'read_only': 'dt_read_only',
            'description': 'dt_description',
        }
        
        # Update all attributes
        for attr_name, data_key in attr_mapping.items():
            setattr(filtered_datatype, attr_name, filtered_data[data_key])
        
        return filtered_datatype
    
    async def _perform_auto_discovery(
        self, 
        datatype: Any, 
        standardized_data: StandardizedOutput, 
        device_id: str
    ) -> Any:
        """
        Perform auto-discovery to update datatype configuration based on standardized data metadata.
        """
        try:
            if not standardized_data.metadata or 'auto_discovery' not in standardized_data.metadata:
                logger.debug(f"No auto-discovery metadata found for device {device_id}")
                return datatype
                
            auto_meta = standardized_data.metadata['auto_discovery']
            logger.debug(f"Processing auto-discovery for datatype {datatype.id} from device {device_id}")
            
            num_values = len(standardized_data.values)
            update_data = {}
            updated = False
            
            # Auto-discovery rules - use existing first value if available, otherwise use suggested
            discovery_rules = {
                'unit': ('suggested_unit', None),
                'data_type': ('suggested_data_type', self._convert_data_types),
                'min': ('suggested_min', None),
                'max': ('suggested_max', None),
            }
            
            for attr_name, (meta_key, converter) in discovery_rules.items():
                existing_values = getattr(datatype, attr_name, [])
                
                if existing_values:
                    # Use existing first value for all positions
                    first_value = existing_values[0]
                    update_data[attr_name] = [first_value] * num_values
                    updated = True
                    logger.debug(f"Auto-discovery: Used existing first {attr_name} for datatype {datatype.id}: {first_value}")
                elif auto_meta.get(meta_key) is not None:
                    # Use suggested values
                    suggested_values = auto_meta[meta_key]
                    if converter:
                        suggested_values = converter(suggested_values, num_values)
                    else:
                        suggested_values = [suggested_values] * num_values if not isinstance(suggested_values, list) else suggested_values
                    
                    update_data[attr_name] = suggested_values
                    updated = True
                    logger.debug(f"Auto-discovery: Updated {attr_name} for datatype {datatype.id}: {suggested_values}")
            
            # Handle labels separately
            if standardized_data.labels:
                update_data['labels'] = standardized_data.labels
                updated = True
                logger.debug(f"Auto-discovery: Updated labels for datatype {datatype.id}: {standardized_data.labels}")
            elif hasattr(datatype, 'labels') and datatype.labels:
                first_label = datatype.labels[0]
                update_data['labels'] = [first_label] * num_values
                updated = True
                logger.debug(f"Auto-discovery: Used existing first label for datatype {datatype.id}: {first_label}")
            
            # Handle other array attributes
            array_attributes = ['persist', 'possible_values', 'whitelist_values', 'description','read_only']
            for attr_name in array_attributes:
                existing_values = getattr(datatype, attr_name, [])
                if existing_values:
                    first_value = existing_values[0]
                    update_data[attr_name] = [first_value] * num_values
                    updated = True
                    logger.debug(f"Auto-discovery: Used existing first {attr_name} for datatype {datatype.id}")
            
            # Apply updates
            if updated and update_data:
                try:
                    for key, value in update_data.items():
                        if hasattr(datatype, key):
                            setattr(datatype, key, value)
                    return datatype
                except Exception as e:
                    logger.error(f"Auto-discovery: Failed to update datatype {datatype.id}: {e}")
                    return datatype
            else:
                logger.debug(f"Auto-discovery: No updates needed for datatype {datatype.id}")
                return datatype
                
        except Exception as e:
            logger.error(f"Auto-discovery error for datatype {datatype.id} from device {device_id}: {e}")
            return datatype
    
    def _convert_data_types(self, suggested_types: List[str], num_values: int) -> List[DatatypeType]:
        """Convert string data types to DatatypeType enums."""
        type_mapping = {
            'integer': DatatypeType.INTEGER,
            'float': DatatypeType.FLOAT,
            'boolean': DatatypeType.BOOLEAN,
            'string': DatatypeType.STRING,
            'datetime': DatatypeType.DATETIME,
            'list': DatatypeType.LIST,
            'object': DatatypeType.OBJECT,
            'bytes': DatatypeType.BYTES,
        }
        
        try:
            type_enums = []
            for dt_str in suggested_types:
                type_enums.append(type_mapping.get(dt_str, DatatypeType.FLOAT))
            
            if type_enums:
                primary_type = type_enums[0]
                return [primary_type] * num_values
            else:
                return [DatatypeType.FLOAT] * num_values
                
        except Exception as e:
            logger.warning(f"Error converting data types for auto-discovery: {e}")
            return [DatatypeType.FLOAT] * num_values
    
    def _align_datatype_with_standardized_data(
        self, 
        datatype: Any, 
        standardized_data: StandardizedOutput, 
        request_id: Optional[str] = None
    ) -> Tuple[Any, StandardizedOutput]:
        """
        Align datatype arrays to match standardized_data.labels order and filter unmatched labels/values.
        """
        try:
            std_labels = standardized_data.labels
            dt_labels = datatype.labels
            
            if not std_labels or not dt_labels:
                logger.debug(f"[{request_id}] No labels to align")
                return datatype, standardized_data
            
            # Create label mapping
            dt_label_to_index = {label: i for i, label in enumerate(dt_labels)}
            
            # Find matched labels
            matched_indices = self._find_matched_label_indices(std_labels, dt_label_to_index)
            
            if not matched_indices['std_indices']:
                logger.error(f"[{request_id}] No matching labels found between standardized_data and datatype")
                return datatype, standardized_data
            
            # Create filtered standardized_data
            filtered_standardized_data = self._create_filtered_standardized_data(
                standardized_data, matched_indices['std_indices']
            )
            
            # Create aligned datatype
            aligned_datatype = self._create_aligned_datatype(
                datatype, matched_indices['dt_indices']
            )
            
            # Log results
            matched_count = len(matched_indices['std_indices'])
            total_count = len(std_labels)
            unmatched_count = total_count - matched_count
            
            if unmatched_count > 0:
                unmatched_labels = [std_labels[i] for i in range(len(std_labels)) 
                                  if i not in matched_indices['std_indices']]
                logger.debug(f"[{request_id}] Unmatched labels filtered out: {unmatched_labels}")
            
            logger.debug(f"[{request_id}] Aligned datatype arrays: {matched_count}/{total_count} labels matched")
            
            return aligned_datatype, filtered_standardized_data
            
        except Exception as e:
            logger.error(f"[{request_id}] Error aligning datatype with standardized_data: {e}")
            return datatype, standardized_data
    
    def _find_matched_label_indices(
        self, 
        std_labels: List[str], 
        dt_label_to_index: Dict[str, int]
    ) -> Dict[str, List[int]]:
        """Find indices of matched labels between standardized_data and datatype."""
        std_indices = []
        dt_indices = []
        
        for std_idx, std_label in enumerate(std_labels):
            if std_label in dt_label_to_index:
                std_indices.append(std_idx)
                dt_indices.append(dt_label_to_index[std_label])
        
        return {
            'std_indices': std_indices,
            'dt_indices': dt_indices
        }
    
    def _create_filtered_standardized_data(
        self, 
        standardized_data: StandardizedOutput, 
        matched_indices: List[int]
    ) -> StandardizedOutput:
        """Create filtered standardized_data with only matched indices."""
        filtered_standardized_data = copy.deepcopy(standardized_data)
        
        # Filter arrays
        filtered_standardized_data.values = [standardized_data.values[i] for i in matched_indices]
        filtered_standardized_data.labels = [standardized_data.labels[i] for i in matched_indices]
        
        if standardized_data.display_names:
            filtered_standardized_data.display_names = [
                standardized_data.display_names[i] for i in matched_indices
            ]
        
        return filtered_standardized_data
    
    def _create_aligned_datatype(self, datatype: Any, matched_dt_indices: List[int]) -> Any:
        """Create aligned datatype with reordered arrays based on matched indices."""
        aligned_datatype = copy.deepcopy(datatype)
        
        # Array attributes to reorder
        array_attributes = [
            'data_type', 'min', 'max', 'unit', 'display_names', 'persist',
            'labels', 'possible_values', 'whitelist_values', 'description', 'read_only'
        ]
        
        for attr_name in array_attributes:
            if hasattr(aligned_datatype, attr_name):
                original_array = getattr(aligned_datatype, attr_name)
                if original_array:
                    reordered_array = self._reorder_array(original_array, matched_dt_indices, attr_name)
                    setattr(aligned_datatype, attr_name, reordered_array)
        
        return aligned_datatype
    
    def _reorder_array(self, array: List[Any], indices: List[int], attr_name: str) -> List[Any]:
        """Reorder array based on matched indices."""
        default_values = {
            'data_type': DatatypeType.FLOAT,
            'unit': "",
            'display_names': "",
            'persist': False,
            'labels': "",
            'possible_values': [],
            'whitelist_values': [],
            'description': "",
            'min': None,
            'max': None
        }
        
        default_value = default_values.get(attr_name)
        
        reordered = []
        for dt_index in indices:
            if dt_index < len(array):
                value = array[dt_index]
                # Special handling for data_type conversion
                if attr_name == 'data_type' and isinstance(value, str):
                    value = self._convert_string_to_datatype_enum(value)
                reordered.append(value)
            else:
                fallback = default_value if default_value is not None else (array[0] if array else None)
                reordered.append(fallback)
        
        return reordered
    
    def _convert_string_to_datatype_enum(self, value: str) -> DatatypeType:
        """Convert string data type to DatatypeType enum."""
        mapping = {
            'integer': DatatypeType.INTEGER,
            'float': DatatypeType.FLOAT,
            'boolean': DatatypeType.BOOLEAN,
            'string': DatatypeType.STRING,
            'datetime': DatatypeType.DATETIME,
            'list': DatatypeType.LIST,
            'object': DatatypeType.OBJECT,
            'bytes': DatatypeType.BYTES,
        }
        return mapping.get(value, DatatypeType.FLOAT)
    
    def _validate_whitelist(
        self, 
        values: List[Any], 
        datatype: Any, 
        standardized_data: StandardizedOutput
    ) -> Tuple[bool, List[bool], List[str]]:
        """
        Check if values match whitelist entries (per-index validation).
        Whitelisted values skip other validations.
        """
        if not values:
            return True, [], []
        
        whitelist_values = getattr(datatype, 'whitelist_values', [])
        if not whitelist_values:
            return True, [False] * len(values), []
        
        labels = standardized_data.labels or []
        whitelist_results = []
        
        for i, value in enumerate(values):
            if value is None:
                whitelist_results.append(False)
                continue
            
            # Get whitelist for this index
            whitelist_for_index = self._get_array_value_at_index(whitelist_values, i)
            
            # Check if value is whitelisted
            is_whitelisted = (whitelist_for_index and str(value) in whitelist_for_index)
            whitelist_results.append(is_whitelisted)
            
            if is_whitelisted:
                label_info = f" (label: {labels[i]})" if i < len(labels) and labels[i] else ""
                logger.debug(f"Value {value} at index {i}{label_info} is whitelisted")
        
        return True, whitelist_results, []
    
    def _validate_type(
        self, 
        values: List[Any], 
        expected_types: List[DatatypeType], 
        standardized_data: StandardizedOutput, 
        whitelist_results: List[bool] = None
    ) -> Tuple[bool, List[Any], List[bool], List[str]]:
        """
        Validate and convert values to expected types (per-index validation).
        Skips validation for whitelisted values.
        """
        if not values:
            return False, [], [], ["No values provided for validation"]
        
        if not expected_types:
            return True, values, [True] * len(values), []
        
        labels = standardized_data.labels or []
        converted_values = []
        type_validation_results = []
        error_messages = []
        all_valid = True
        
        for i, value in enumerate(values):
            label_info = f" (label: {labels[i]})" if i < len(labels) and labels[i] else ""
            
            # Skip type validation for whitelisted values
            if whitelist_results and i < len(whitelist_results) and whitelist_results[i]:
                converted_values.append(value)
                type_validation_results.append(True)
                logger.debug(f"Value {value} at index {i}{label_info} is whitelisted, skipping type validation")
                continue
            
            if value is None:
                converted_values.append(None)
                type_validation_results.append(False)
                error_messages.append(f"Value at index {i}{label_info} is None")
                all_valid = False
                continue
            
            # Get expected type for this index
            expected_type = self._get_array_value_at_index(expected_types, i, DatatypeType.FLOAT)
            
            # Convert value
            converted_value, conversion_success, error_msg = self._convert_value_to_type(
                value, expected_type, i, label_info
            )
            
            converted_values.append(converted_value)
            type_validation_results.append(conversion_success)
            
            if not conversion_success:
                error_messages.append(error_msg)
                all_valid = False
                logger.warning(f"Value {value} at index {i}{label_info} failed type validation (expected: {expected_type})")
            else:
                logger.debug(f"Value {value} at index {i}{label_info} passed type validation (expected: {expected_type})")
        
        return all_valid, converted_values, type_validation_results, error_messages
    
    def _convert_value_to_type(
        self, 
        value: Any, 
        expected_type: DatatypeType, 
        index: int, 
        label_info: str
    ) -> Tuple[Any, bool, str]:
        """Convert a single value to the expected type."""
        try:
            if expected_type == DatatypeType.INTEGER:
                return int(float(value)), True, ""  # Convert to float first to handle string decimals, then to int
            elif expected_type == DatatypeType.FLOAT:
                return float(value), True, ""
            elif expected_type == DatatypeType.BOOLEAN:
                if isinstance(value, bool):
                    return bool(value), True, ""
                elif isinstance(value, str):
                    return value.lower() in ('true', 'yes', '1'), True, ""
                else:
                    return bool(value), True, ""
            elif expected_type == DatatypeType.STRING:
                return str(value), True, ""
            elif expected_type == DatatypeType.DATETIME:
                return value, True, ""  # Keep original for now
            elif expected_type == DatatypeType.LIST:
                return value if isinstance(value, list) else [value], True, ""
            elif expected_type == DatatypeType.OBJECT:
                if isinstance(value, str):
                    try:
                        return json.loads(value), True, ""
                    except json.JSONDecodeError:
                        return value, True, ""
                else:
                    return value, True, ""
            elif expected_type == DatatypeType.BYTES:
                if isinstance(value, bytes):
                    return value, True, ""
                elif isinstance(value, str):
                    return value.encode('utf-8'), True, ""
                else:
                    return str(value).encode('utf-8'), True, ""
            else:
                logger.info(f"Using default validation for unrecognized type: {expected_type} at index {index}{label_info}")
                return value, True, ""
                
        except (ValueError, TypeError) as e:
            error_msg = f"Type validation failed for value '{value}' at index {index}: expected {expected_type}. Error: {e}"
            return value, False, error_msg
    
    def _validate_range(
        self, 
        values: List[Any], 
        datatype: Any, 
        standardized_data: StandardizedOutput, 
        whitelist_results: List[bool] = None
    ) -> Tuple[bool, List[bool], List[str]]:
        """
        Check if values are within range constraints (per-index validation).
        Skips validation for whitelisted values.
        """
        if not values:
            return False, [], ["No values provided for range validation"]
        
        min_values = getattr(datatype, 'min', [])
        max_values = getattr(datatype, 'max', [])
        
        if not min_values and not max_values:
            return True, [True] * len(values), []
        
        labels = standardized_data.labels or []
        validation_results = []
        error_messages = []
        all_valid = True
        
        for i, value in enumerate(values):
            label_info = f" (label: {labels[i]})" if i < len(labels) and labels[i] else ""
            
            # Skip range validation for whitelisted values
            if whitelist_results and i < len(whitelist_results) and whitelist_results[i]:
                validation_results.append(True)
                logger.debug(f"Value {value} at index {i}{label_info} is whitelisted, skipping range validation")
                continue
            
            if value is None:
                validation_results.append(False)
                error_messages.append(f"Value at index {i}{label_info} is None")
                all_valid = False
                continue
            
            # Get min/max constraints for this index
            min_val = self._get_array_value_at_index(min_values, i)
            max_val = self._get_array_value_at_index(max_values, i)
            
            # Skip validation if no constraints
            if min_val is None and max_val is None:
                logger.debug(f"No min/max constraints for value at index {i}{label_info}, skipping range validation")
                validation_results.append(True)
                continue
            
            # Validate range
            range_valid, range_error = self._check_value_in_range(value, min_val, max_val, i, label_info)
            validation_results.append(range_valid)
            
            if not range_valid:
                error_messages.append(range_error)
                all_valid = False
                logger.warning(f"Value {value} at index {i}{label_info} failed range validation")
            else:
                constraints = []
                if min_val is not None:
                    constraints.append(f"min={min_val}")
                if max_val is not None:
                    constraints.append(f"max={max_val}")
                logger.debug(f"Value {value} at index {i}{label_info} passed range validation ({', '.join(constraints)})")
        
        return all_valid, validation_results, error_messages
    
    def _check_value_in_range(
        self, 
        value: Any, 
        min_val: Any, 
        max_val: Any, 
        index: int, 
        label_info: str
    ) -> Tuple[bool, str]:
        """Check if a single value is within the specified range."""
        try:
            value_float = float(value)
            
            # Check min constraint
            if min_val is not None:
                try:
                    min_float = float(min_val)
                    if value_float < min_float:
                        return False, f"Value {value} at index {index}{label_info} is below minimum {min_val}"
                except (ValueError, TypeError) as e:
                    logger.warning(f"Invalid min constraint '{min_val}' at index {index}{label_info}: {e}")
            
            # Check max constraint
            if max_val is not None:
                try:
                    max_float = float(max_val)
                    if value_float > max_float:
                        return False, f"Value {value} at index {index}{label_info} is above maximum {max_val}"
                except (ValueError, TypeError) as e:
                    logger.warning(f"Invalid max constraint '{max_val}' at index {index}{label_info}: {e}")
            
            return True, ""
            
        except (ValueError, TypeError) as e:
            return False, f"Range validation failed due to type conversion error: {value} at index {index}{label_info} - {str(e)}"
    
    def _get_array_value_at_index(self, array: List[Any], index: int, default: Any = None) -> Any:
        """Get value from array at index, using last available value or default if index is out of bounds."""
        if not array:
            return default
        
        if index < len(array):
            return array[index]
        elif array:
            return array[-1]  # Use last available value
        else:
            return default
    
    def _build_validated_output(
        self, 
        device_id: str, 
        datatype: Any, 
        converted_values: List[Any], 
        request_id: Optional[str] = None,
        standardized_data: Optional[StandardizedOutput] = None,
    ) -> ValidatedOutput:
        """Build a ValidatedOutput object from validated data."""
        
        # Convert DatatypeType enums to strings for JSON serialization
        datatype_type_strings = []
        if datatype.data_type:
            datatype_type_strings = [
                str(dt.value) if hasattr(dt, 'value') else str(dt) 
                for dt in datatype.data_type
            ]
        
        # Build metadata
        metadata = {
            "datatype_id": datatype.id,
            "datatype_name": datatype.name,
            "datatype_unit": datatype.unit,
            "datatype_type": datatype_type_strings,
            "datatype_category": str(datatype.category),
            "datatype_possible_values": datatype.possible_values,
            "datatype_whitelist_values": datatype.whitelist_values,
            "datatype_read_only": datatype.read_only,
            "datatype_description": datatype.description,
            "datatype_min": datatype.min,
            "datatype_max": datatype.max,
            "persist": datatype.persist,
            **standardized_data.metadata,
        }
        
        return ValidatedOutput(
            device_id=device_id,
            values=converted_values,
            labels=datatype.labels,
            display_names=datatype.display_names,
            index=datatype.datatype_index,
            request_id=request_id,
            timestamp=standardized_data.timestamp,
            metadata=metadata,
        )
    
    def _process_display_name_replacement(
        self, 
        source_display_names: Optional[List[str]], 
        datatype_display_names: List[str]
    ) -> List[str]:
        """
        Process display name replacement by replacing %name% placeholders with actual datatype display names.
        """
        if not source_display_names:
            return datatype_display_names
        
        # Handle %name% placeholder replacement
        if "%name%" in str(source_display_names):
            if isinstance(source_display_names, list) and isinstance(datatype_display_names, list):
                updated_display_names = []
                for i, template in enumerate(source_display_names):
                    if i < len(datatype_display_names):
                        replacement = datatype_display_names[i] if datatype_display_names[i] else ""
                        updated_display_names.append(str(template).replace("%name%", replacement))
                    else:
                        # No corresponding datatype display name, remove placeholder
                        updated_display_names.append(str(template).replace("%name%", ""))
                return updated_display_names
        
        return source_display_names if source_display_names else datatype_display_names