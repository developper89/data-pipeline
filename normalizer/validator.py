# normalizer/validator.py

import logging
from typing import Any, Dict, List, Optional, Union, Tuple
from datetime import datetime
import json
from shared.models.common import StandardizedOutput, ValidatedOutput, ErrorMessage
from preservarium_sdk.infrastructure.sql_repository.sql_datatype_repository import SQLDatatypeRepository
from preservarium_sdk.domain.model.datatype import DatatypeType

logger = logging.getLogger(__name__)

class Validator:
    """
    Validates and normalizes sensor data based on configurable rules
    stored in the datatypes table.
    
    Note: The validator supports None values in min/max constraint arrays,
    where None indicates no constraint for that specific array index.
    """
    
    def __init__(self, datatype_repository):
        """
        Initialize the validator with repository for accessing validation rules.
        """
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
            datatype_id: The ID of the datatype to validate against
            standardized_data: StandardizedOutput object from the parser script
            request_id: Optional request ID for tracing
            datatype: Optional datatype object (if already fetched)
            
        Returns:
            Tuple with validated StandardizedOutput (or None if validation failed)
            and a list of any validation errors
        """
        errors = []
        
        # Clone datatype to avoid modifying the original object
        import copy
        datatype = copy.deepcopy(datatype)
        
        datatype_name = str(datatype.category) if len(list(dict.fromkeys(datatype.display_names))) > 1 else datatype.display_names[0]
        datatype.name = datatype_name
        
        # Values to be processed
        values = standardized_data.values

        if standardized_data.display_names:
            # Use suggested display names with processing
            processed_display_names = self._process_display_name_replacement(
                standardized_data.display_names, [datatype.name] * len(values)
            )
            datatype.display_names = processed_display_names
            logger.info(f"Updated display names for datatype {datatype.id}: {processed_display_names}")
        # else:
        #     datatype.display_names = [datatype.name] * len(values)
        #     logger.info(f"Used existing display name for datatype {datatype.id}: {datatype.name}")
            
        # Check for auto-discovery and update datatype if enabled
        if datatype and datatype.auto_discovery:
            datatype = await self._perform_auto_discovery(datatype, standardized_data, device_id)
        
        # 3. Apply any parsing operations first
        # parsing_applied = False
        
        # parsing_valid, parsed_values, parsing_errors = self._apply_parsing_operations(
        #     values,
        #     validation_params
        # )
        
        # Add any parsing operation errors
        # for error_msg in parsing_errors:
        #     errors.append(ErrorMessage(
        #         error=error_msg,
        #         original_message={"device_id": device_id},
        #         request_id=request_id
        #     ))
        
        # if parsing_valid and parsed_values:
        #     # Use the parsed values for further validation
        #     values = parsed_values
        #     parsing_applied = True
        # logger.debug(f"parsing_applied: {parsing_applied}, parsed_values: {parsed_values}")
        # 4. Validate all values against the datatype
        # data_type is now List[DatatypeType], pass full array for per-index validation
        expected_types = []
        if hasattr(datatype, 'data_type') and datatype.data_type:
            expected_types = datatype.data_type

        type_valid, converted_values, type_validation_results, type_errors = self._validate_type(
            values, 
            expected_types,
            standardized_data
        )

        # Add any type validation errors
        for error_msg in type_errors:
            errors.append(ErrorMessage(
                error=error_msg,
                original_message={"device_id": device_id},
                request_id=request_id
            ))
        
        # 5. Validate range for all values
        range_valid, range_results, range_errors = self._validate_range(
            converted_values,
            datatype,
            standardized_data
        )

        # Add any range validation errors
        for error_msg in range_errors:
            errors.append(ErrorMessage(
                error=error_msg,
                original_message={"device_id": device_id},
                request_id=request_id
            ))
        
        # 6. Build validated output with partial validation support
        # Filter out invalid values but keep valid ones
        if converted_values and range_results:
            # Combine type and range validation results to determine which values are valid
            valid_indices = []
            for i in range(len(converted_values)):
                # A value is valid if it passes both type conversion and range validation
                type_valid_for_index = (i < len(type_validation_results) and type_validation_results[i])
                range_valid_for_index = (i < len(range_results) and range_results[i])
                
                if type_valid_for_index and range_valid_for_index:
                    valid_indices.append(i)
            
            if valid_indices:
                # Filter values, labels, display_names, and other arrays to only include valid indices
                filtered_values = [converted_values[i] for i in valid_indices]
                filtered_labels = [standardized_data.labels[i] for i in valid_indices] if standardized_data.labels else []
                filtered_display_names = [standardized_data.display_names[i] for i in valid_indices] if standardized_data.display_names else []
                
                # Filter datatype arrays to match the valid indices
                filtered_datatype_labels = [datatype.labels[i] for i in valid_indices] if datatype.labels else []
                filtered_datatype_display_names = [datatype.display_names[i] for i in valid_indices] if datatype.display_names else []
                filtered_datatype_units = [datatype.unit[i] for i in valid_indices] if datatype.unit else []
                filtered_datatype_persist = [datatype.persist[i] for i in valid_indices] if datatype.persist else []
                
                # Create a filtered standardized_data object for building the validated output
                filtered_standardized_data = StandardizedOutput(
                    device_id=standardized_data.device_id,
                    values=filtered_values,
                    labels=filtered_labels,
                    display_names=filtered_display_names,
                    index=standardized_data.index,
                    metadata=standardized_data.metadata,
                    request_id=standardized_data.request_id,
                    timestamp=standardized_data.timestamp
                )
                
                # Create a filtered datatype object for building the validated output
                # Copy the original datatype object and update its arrays
                import copy
                filtered_datatype = copy.deepcopy(datatype)
                
                # Set filtered arrays
                filtered_datatype.labels = filtered_datatype_labels
                filtered_datatype.display_names = filtered_datatype_display_names
                filtered_datatype.unit = filtered_datatype_units
                filtered_datatype.persist = filtered_datatype_persist
                
                # Build validated output with filtered data
                validated_output = self._build_validated_output(
                    device_id, 
                    filtered_datatype,
                    filtered_values,
                    request_id,
                    filtered_standardized_data,
                )
                
                logger.info(f"[{request_id}] Partial validation: {len(valid_indices)} out of {len(converted_values)} values passed validation for device {device_id}")
                return validated_output, errors
        
        # If no valid values found, return None
        logger.warning(f"[{request_id}] No valid values found after validation for device {device_id}")
        return None, errors
    
    async def _perform_auto_discovery(
        self, 
        datatype: Any, 
        standardized_data: StandardizedOutput, 
        device_id: str
    ) -> Any:
        """
        Perform auto-discovery to update datatype configuration for min, max, unit, data_type, display_names, and labels
        based on information from standardized data metadata.
        
        Args:
            datatype: The datatype object to update
            standardized_data: StandardizedOutput containing auto-discovery metadata
            device_id: The device ID for logging
            
        Returns:
            The updated datatype object
        """
        try:
            # Extract auto-discovery metadata from standardized data
            if not standardized_data.metadata or 'auto_discovery' not in standardized_data.metadata:
                logger.debug(f"No auto-discovery metadata found for device {device_id}")
                return datatype
                
            auto_meta = standardized_data.metadata['auto_discovery']
            logger.debug(f"Processing auto-discovery for datatype {datatype.id} from device {device_id}")
            
            # Prepare update data dictionary
            update_data = {}
            updated = False
            
            # Get the number of values for creating arrays
            num_values = len(standardized_data.values)
            
            # 1. Handle units - use existing first value if available, otherwise use suggested value
            # Check if datatype already has unit values
            if hasattr(datatype, 'unit') and datatype.unit and len(datatype.unit) > 0:
                # Use first existing unit value for all values
                first_unit = datatype.unit[0]
                update_data['unit'] = [first_unit] * num_values
                updated = True
                logger.debug(f"Auto-discovery: Used existing first unit value for datatype {datatype.id}: {first_unit}")
            elif auto_meta.get('suggested_unit'):
                suggested_units = auto_meta['suggested_unit']
                if suggested_units:
                    # Use suggested units
                    update_data['unit'] = suggested_units
                    updated = True
                    logger.debug(f"Auto-discovery: Updated units for datatype {datatype.id}: {suggested_units}")
            
            # 2. Handle data types - use existing first value if available, otherwise use suggested value
            # Check if datatype already has data_type values
            if hasattr(datatype, 'data_type') and datatype.data_type and len(datatype.data_type) > 0:
                # Use first existing data type for all values
                first_data_type = datatype.data_type[0]
                update_data['data_type'] = [first_data_type] * num_values
                updated = True
                logger.debug(f"Auto-discovery: Used existing first data type for datatype {datatype.id}: {first_data_type}")
            elif auto_meta.get('suggested_data_type'):
                suggested_types = auto_meta['suggested_data_type']
                # Convert string data types to DatatypeType enums
                from preservarium_sdk.domain.model.datatype import DatatypeType
                try:
                    # Use suggested data types
                    type_enums = []
                    for dt_str in suggested_types:
                        if dt_str == 'number':
                            type_enums.append(DatatypeType.NUMBER)
                        elif dt_str == 'boolean':
                            type_enums.append(DatatypeType.BOOLEAN)
                        elif dt_str == 'string':
                            type_enums.append(DatatypeType.STRING)
                        else:
                            type_enums.append(DatatypeType.NUMBER)  # Default fallback
                    
                    if type_enums:
                        # Create data_type list with same length as values, using the first detected type
                        primary_type = type_enums[0] if type_enums else DatatypeType.NUMBER
                        update_data['data_type'] = [primary_type] * num_values
                        updated = True
                        logger.debug(f"Auto-discovery: Updated data types for datatype {datatype.id}: {[str(primary_type)] * num_values}")
                except Exception as e:
                    logger.warning(f"Error converting data types for auto-discovery: {e}")
            
            # 3. Handle min constraint - use existing first value if available, otherwise use suggested value
            # Check if datatype already has min values
            if hasattr(datatype, 'min') and datatype.min and len(datatype.min) > 0:
                # Use first existing min value for all values
                first_min = datatype.min[0]
                update_data['min'] = [first_min] * num_values
                updated = True
                logger.debug(f"Auto-discovery: Used existing first min value for datatype {datatype.id}: {first_min}")
            elif auto_meta.get('suggested_min') is not None:
                suggested_min = auto_meta['suggested_min']
                # Use suggested min
                update_data['min'] = [suggested_min] * num_values
                updated = True
                logger.debug(f"Auto-discovery: Updated min constraint for datatype {datatype.id}: {suggested_min}")
            
            # 4. Handle max constraint - use existing first value if available, otherwise use suggested value
            # Check if datatype already has max values
            if hasattr(datatype, 'max') and datatype.max and len(datatype.max) > 0:
                # Use first existing max value for all values
                first_max = datatype.max[0]
                update_data['max'] = [first_max] * num_values
                updated = True
                logger.debug(f"Auto-discovery: Used existing first max value for datatype {datatype.id}: {first_max}")
            elif auto_meta.get('suggested_max') is not None:
                suggested_max = auto_meta['suggested_max']
                # Use suggested max
                update_data['max'] = [suggested_max] * num_values
                updated = True
                logger.debug(f"Auto-discovery: Updated max constraint for datatype {datatype.id}: {suggested_max}")
            
            # 5. Handle display names - use existing first value if available, otherwise use suggested value
            # Check if datatype already has display_names values
            
            
            
            # 6. Handle labels - use existing first value if available, otherwise use suggested value
            # Check if datatype already has labels values
            
            if standardized_data.labels:
                # Use suggested labels
                update_data['labels'] = standardized_data.labels
                updated = True
                logger.debug(f"Auto-discovery: Updated labels for datatype {datatype.id}: {standardized_data.labels}")
            elif hasattr(datatype, 'labels') and datatype.labels and len(datatype.labels) > 0:
                # Use first existing label for all values
                first_label = datatype.labels[0]
                update_data['labels'] = [first_label] * num_values
                updated = True
                logger.debug(f"Auto-discovery: Used existing first label for datatype {datatype.id}: {first_label}")
            
            if hasattr(datatype, 'persist') and datatype.persist:
                first_persist = datatype.persist[0]
                update_data['persist'] = [first_persist] * num_values
                updated = True
                logger.debug(f"Auto-discovery: Set persist to True for datatype {datatype.id}")
            # elif auto_meta.get('suggested_persist') is not None:
            #     update_data['persist'] = [auto_meta['suggested_persist']] * num_values
            #     updated = True
            #     logger.info(f"Auto-discovery: Updated persist for datatype {datatype.id}: {auto_meta['suggested_persist']}")
            
            # 7. Set auto_discovery to False after performing discovery
            # update_data['auto_discovery'] = False
            # updated = True
            logger.debug(f"Auto-discovery: Set auto_discovery to False for datatype {datatype.id}")
            
            # Apply updates to the datatype if any changes were made
            if updated and update_data:
                try:
                    # Update the local datatype object to reflect changes for current validation
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
            return datatype  # Return original datatype even if auto-discovery fails

    def _validate_range(self, values: List[Any], datatype: Any, standardized_data: StandardizedOutput) -> Tuple[bool, List[bool], List[str]]:
        """
        Check if values are within the range defined in datatype.
        Uses per-index validation: each value is checked against min/max at the same index.
        
        Note: min and max arrays can contain None values to indicate
        no constraint for that specific index.
        
        Args:
            values: The list of values to validate
            datatype: The datatype object containing min and max constraints
            standardized_data: StandardizedOutput object containing labels
            
        Returns:
            Tuple of (all_valid, validation_results, error_messages)
        """
        if not values:
            return False, [], ["No values provided for range validation"]
        
        # Get min and max value arrays from datatype
        min_values = getattr(datatype, 'min', [])
        max_values = getattr(datatype, 'max', [])
        
        # Get labels from standardized_data for better error messages
        labels = standardized_data.labels if standardized_data.labels else []
        
        # If no range constraints are defined, consider all values valid
        if not min_values and not max_values:
            return True, [True] * len(values), []
        
        # Validate each value using corresponding index constraints
        all_valid = True
        validation_results = []
        error_messages = []
        
        for i, value in enumerate(values):
            # Get label for this index (labels array is same length as values)
            label = labels[i] if labels and i < len(labels) else ""
            
            # Format label info for error messages
            label_info = f" (label: {label})" if label else ""
            
            if value is None:
                validation_results.append(False)
                error_messages.append(f"Value at index {i}{label_info} is None")
                all_valid = False
                continue
            
            # Get min/max for this index (use last available if arrays are shorter)
            min_val = None
            max_val = None
            
            if min_values and i < len(min_values):
                min_val = min_values[i]
            elif min_values:
                # Use last available min value for remaining indices
                min_val = min_values[-1]
                
            if max_values and i < len(max_values):
                max_val = max_values[i]
            elif max_values:
                # Use last available max value for remaining indices
                max_val = max_values[-1]
            
            # Skip validation if no constraints for this value (both are None)
            if min_val is None and max_val is None:
                logger.debug(f"No min/max constraints for value at index {i}{label_info}, skipping range validation")
                validation_results.append(True)
                continue
            
            try:
                value_float = float(value)
                valid = True
                
                # Check min constraint if available (not None)
                if min_val is not None:
                    try:
                        min_float = float(min_val)
                        if value_float < min_float:
                            valid = False
                            error_messages.append(f"Value {value} at index {i}{label_info} is below minimum {min_val}")
                    except (ValueError, TypeError) as e:
                        logger.warning(f"Invalid min constraint '{min_val}' at index {i}{label_info}: {e}")
                        # Continue validation with max constraint if available
                
                # Check max constraint if available (not None)
                if max_val is not None:
                    try:
                        max_float = float(max_val)
                        if value_float > max_float:
                            valid = False
                            error_messages.append(f"Value {value} at index {i}{label_info} is above maximum {max_val}")
                    except (ValueError, TypeError) as e:
                        logger.warning(f"Invalid max constraint '{max_val}' at index {i}{label_info}: {e}")
                        # Continue validation - this constraint is ignored
                
                validation_results.append(valid)
                if not valid:
                    all_valid = False
                    # Log failed validation as warning
                    constraints = []
                    if min_val is not None:
                        constraints.append(f"min={min_val}")
                    if max_val is not None:
                        constraints.append(f"max={max_val}")
                    logger.warning(f"Value {value} at index {i}{label_info} failed range validation ({', '.join(constraints)})")
                else:
                    # Log successful validation for debugging
                    constraints = []
                    if min_val is not None:
                        constraints.append(f"min={min_val}")
                    if max_val is not None:
                        constraints.append(f"max={max_val}")
                    logger.debug(f"Value {value} at index {i}{label_info} passed range validation ({', '.join(constraints)})")
                    
            except (ValueError, TypeError) as e:
                validation_results.append(False)
                error_messages.append(f"Range validation failed due to type conversion error: {value} at index {i}{label_info} - {str(e)}")
                all_valid = False
        
        return all_valid, validation_results, error_messages
        
    def _validate_type(self, values: List[Any], expected_types: List[DatatypeType], standardized_data: StandardizedOutput) -> Tuple[bool, List[Any], List[bool], List[str]]:
        """
        Validate and convert a list of values to the expected type if possible.
        Uses per-index validation: each value is validated against the type at the same index.
        
        Args:
            values: The list of values to validate and convert
            expected_types: List of expected data types (DatatypeType enums)
            standardized_data: StandardizedOutput object containing labels
            
        Returns:
            Tuple of (all_valid, converted_values, type_validation_results, error_messages)
        """
        if not values:
            return False, [], [], ["No values provided for validation"]
        
        if not expected_types:
            # If no expected types are specified, consider any values valid
            return True, values, [True] * len(values), []
        
        # Get labels from standardized_data for better error messages
        labels = standardized_data.labels if standardized_data.labels else []
        
        converted_values = []
        type_validation_results = []
        error_messages = []
        all_valid = True
        
        for i, value in enumerate(values):
            # Get label for this index (labels array is same length as values)
            label = labels[i] if labels and i < len(labels) else ""
            
            # Format label info for error messages
            label_info = f" (label: {label})" if label else ""
            
            if value is None:
                converted_values.append(None)
                type_validation_results.append(False)
                error_messages.append(f"Value at index {i}{label_info} is None")
                logger.warning(f"Value at index {i}{label_info} failed type validation (value is None)")
                all_valid = False
                continue
            
            # Get expected type for this index (use last available if array is shorter)
            if i < len(expected_types):
                expected_type = expected_types[i]
            elif len(expected_types) > 0:
                expected_type = expected_types[-1]  # Use last type for remaining values
            else:
                # No type constraint, keep original value
                converted_values.append(value)
                type_validation_results.append(True)
                logger.debug(f"Value {value} at index {i}{label_info} passed type validation (no constraint)")
                continue
            
            type_valid_for_this_value = True
            try:
                if expected_type == DatatypeType.NUMBER:
                    converted_values.append(float(value))
                elif expected_type == DatatypeType.BOOLEAN:
                    if isinstance(value, bool):
                        converted_values.append(bool(value))
                    elif isinstance(value, str):
                        converted_values.append(value.lower() in ('true', 'yes', '1'))
                    else:
                        converted_values.append(bool(value))
                elif expected_type == DatatypeType.STRING:
                    converted_values.append(str(value))
                elif expected_type == DatatypeType.DATETIME:
                    # For datetime, we'll keep the original value for now
                    # Additional datetime parsing logic can be added here if needed
                    converted_values.append(value)
                elif expected_type == DatatypeType.LIST:
                    # For list type, ensure it's a list or convert to list
                    if isinstance(value, list):
                        converted_values.append(value)
                    else:
                        converted_values.append([value])
                elif expected_type == DatatypeType.OBJECT:
                    # For object type, try to parse as JSON if string, otherwise keep as-is
                    if isinstance(value, str):
                        try:
                            converted_values.append(json.loads(value))
                        except json.JSONDecodeError:
                            converted_values.append(value)
                    else:
                        converted_values.append(value)
                elif expected_type == DatatypeType.BYTES:
                    # For bytes type, convert to bytes if possible
                    if isinstance(value, bytes):
                        converted_values.append(value)
                    elif isinstance(value, str):
                        converted_values.append(value.encode('utf-8'))
                    else:
                        converted_values.append(str(value).encode('utf-8'))
                else:
                    # Default case - no conversion
                    logger.info(f"Using default validation for unrecognized type: {expected_type} at index {i}{label_info}")
                    converted_values.append(value)
            except (ValueError, TypeError) as e:
                logger.warning(f"Type conversion failed for value {value} at index {i}{label_info}: expected {expected_type}. Error: {e}")
                converted_values.append(value)  # Keep original value
                error_messages.append(f"Type validation failed for value '{value}' at index {i}: expected {expected_type}. Error: {e}")
                type_valid_for_this_value = False
                all_valid = False
            
            # Log validation result
            if type_valid_for_this_value:
                logger.debug(f"Value {value} at index {i}{label_info} passed type validation (expected: {expected_type})")
            else:
                logger.warning(f"Value {value} at index {i}{label_info} failed type validation (expected: {expected_type})")
            
            type_validation_results.append(type_valid_for_this_value)
        
        return all_valid, converted_values, type_validation_results, error_messages
                
    def _build_validated_output(
        self, 
        device_id: str, 
        datatype: Any, 
        converted_values: List[Any], 
        request_id: Optional[str] = None,
        standardized_data: Optional[StandardizedOutput] = None,
    ) -> ValidatedOutput:
        """
        Build a ValidatedOutput object from validated data.
        Also creates a ValidatedOutput object for internal validation tracking.
        
        Args:
            device_id: The device ID
            datatype: The datatype model
            converted_values: List of validated and converted values
            request_id: Optional request ID
            standardized_data: Optional StandardizedOutput object
        Returns:
            ValidatedOutput object with validated data
        """

        # Build metadata with validation info
        # Always create base metadata
        
        # Convert DatatypeType enums to strings for JSON serialization
        datatype_type_strings = []
        if datatype.data_type:
            datatype_type_strings = [str(dt.value) if hasattr(dt, 'value') else str(dt) for dt in datatype.data_type]
        
        metadata = {
            "datatype_id": datatype.id,
            "datatype_name": datatype.name,
            "datatype_unit": datatype.unit,
            "datatype_type": datatype_type_strings,
            "datatype_category": str(datatype.category),
            "persist": datatype.persist,
        }
        
        # Create ValidatedOutput for internal tracking (no metadata)
        validated_output = ValidatedOutput(
            device_id=device_id,
            values=converted_values,
            labels=datatype.labels,
            display_names=datatype.display_names,
            index=datatype.datatype_index,
            request_id=request_id,
            timestamp=standardized_data.timestamp,
            metadata=metadata,
        )
        
        return validated_output

    def _apply_parsing_operations(self, values: List[Any], validation_params: Dict) -> Tuple[bool, List[Any], List[str]]:
        """
        Apply parsing operations to values based on per-index operation parameters.
        Each value uses the parsing operation at the corresponding index.
        
        Args:
            values: List of values to apply operations to
            validation_params: Dictionary of validation parameters
            
        Returns:
            Tuple of (success, parsed_values, error_messages)
        """
        if not values:
            return True, [], []
        
        # Look for the operation parameter array
        parsing_ops = validation_params.get("parsing_ops", [])
        if not parsing_ops:
            # No operations to apply
            return True, values, []
        
        # Initialize result variables
        success = True
        parsed_values = []
        error_messages = []
        
        try:
            # Process each value with the corresponding parsing operation
            for i, value in enumerate(values):
                if value is None:
                    parsed_values.append(None)
                    continue
                
                # Get parsing operation for this index (use last available if array is shorter)
                if i < len(parsing_ops):
                    parsing_op = parsing_ops[i]
                elif len(parsing_ops) > 0:
                    parsing_op = parsing_ops[-1]  # Use last operation for remaining values
                else:
                    # No parsing operation available, keep original value
                    parsed_values.append(value)
                    continue
                
                # Skip if parsing operation is empty
                if not parsing_op:
                    parsed_values.append(value)
                    continue
                    
                try:
                    # Replace %val% in the formula with the actual value
                    formula = parsing_op.replace("%val%", str(value))
                    
                    # Evaluate the formula safely
                    # This handles operations like "(%val%/100)" or "(%val%-10000)/100"
                    result = eval(formula, {"__builtins__": {}})
                    parsed_values.append(result)
                    
                    logger.debug(f"Applied operation '{parsing_op}' to value '{value}' at index {i} with result: {result}")
                    
                except (ValueError, TypeError, SyntaxError, NameError) as e:
                    error_messages.append(f"Failed to apply operation '{parsing_op}' to value '{value}' at index {i}: {str(e)}")
                    parsed_values.append(value)  # Keep original value
                    success = False
                    
        except Exception as e:
            # Handle unexpected errors
            error_messages.append(f"Error applying operations: {str(e)}")
            return False, values, error_messages
        
        return success, parsed_values, error_messages

    def _process_display_name_replacement(self, source_display_names: Optional[List[str]], datatype_display_names: List[str]) -> List[str]:
        """
        Process display name replacement by replacing %name% placeholders with actual datatype display names.
        
        Args:
            source_display_names: Optional list of display names that may contain placeholders
            datatype_display_names: The datatype object containing display_names as List[str]
            
        Returns:
            List of processed display names (always returns a list, even if empty)
        """
        
        # If no source_display_names provided, use datatype display_names directly
        if not source_display_names:
            return datatype_display_names
        # Replace %name% placeholder in display_names with datatype.display_names
        if source_display_names and "%name%" in str(source_display_names):
            # Get datatype.display_names as List[str] directly
            # Handle list replacement - both should be lists with matching indexes
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

        # If no replacement needed, return source_display_names as is
        return source_display_names if source_display_names else datatype_display_names

    def _format_label(self, standardized_data: Optional[StandardizedOutput], datatype: Any) -> List[str]:
        """
        Format and return the label list from standardized_data or datatype.
        
        Args:
            standardized_data: Optional StandardizedOutput object containing labels
            datatype: The datatype object containing labels as List[str]
            
        Returns:
            List of label strings (always returns a list, even if empty)
        """
        # Get unit label from standardized_data first, then datatype if needed
        if standardized_data and standardized_data.labels is not None:
            # Ensure it's a list of strings
            if isinstance(standardized_data.labels, list):
                return [str(item) for item in standardized_data.labels]
            else:
                return [str(standardized_data.labels)]
        elif hasattr(datatype, 'labels') and datatype.labels:
            try:
                # labels is now a List[str], not a JSON string
                datatype_labels = datatype.labels
                # Ensure it's always a list of strings
                if isinstance(datatype_labels, list):
                    return [str(item) for item in datatype_labels]
                else:
                    return [str(datatype_labels)] if datatype_labels is not None else []
            except (TypeError):
                return []
        
        return []
