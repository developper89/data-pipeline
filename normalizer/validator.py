# normalizer/validator.py

import logging
from typing import Any, Dict, List, Optional, Union, Tuple
from datetime import datetime
import json
from shared.models.common import StandardizedOutput, ValidatedOutput, ErrorMessage
from preservarium_sdk.infrastructure.sql_repository.sql_datatype_repository import SQLDatatypeRepository
from preservarium_sdk.infrastructure.sql_repository.sql_field_repository import SQLFieldRepository

logger = logging.getLogger(__name__)

class Validator:
    """
    Validates and normalizes sensor data based on configurable rules
    stored in the datatypes and datatypes_data tables.
    """
    
    def __init__(self, datatype_repository, field_repository):
        """
        Initialize the validator with repositories for accessing validation rules.
        """
        self.datatype_repository = datatype_repository
        self.field_repository = field_repository
        
    async def validate_and_normalize(
        self, 
        device_id: str,
        datatype_id: str,
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
        
        # 2. Get validation parameters from datatypes_data
        validation_params = await self._get_validation_params(datatype_id)
        
        # 3. Validate all values against the datatype
        type_valid, converted_values, type_errors = self._validate_type(
            standardized_data.values, 
            datatype.data_type
        )
        
        # Add any type validation errors
        for error_msg in type_errors:
            errors.append(ErrorMessage(
                error=error_msg,
                original_message={"device_id": device_id},
                request_id=request_id
            ))
        
        # 4. Validate range for all values
        range_valid, range_results, range_errors = self._validate_range(
            converted_values,
            validation_params
        )
        
        # Add any range validation errors
        for error_msg in range_errors:
            errors.append(ErrorMessage(
                error=error_msg,
                original_message={"device_id": device_id},
                request_id=request_id
            ))
        
        # 6. Build standardized output if all validations pass or if partial validation is allowed
        if (type_valid and range_valid) or self._allow_partial_validation(validation_params):
            # Apply any needed transformations
            normalized_output = self._build_standardized_output(
                device_id, 
                datatype,
                converted_values,
                validation_params,
                request_id,
                standardized_data.timestamp
            )
            return normalized_output, errors
        
        return None, errors
    
    async def _get_validation_params(self, datatype_id: str) -> Dict[str, Any]:
        """
        Fetches validation parameters from datatypes_data table for a specific datatype.
        Converts field values to a dictionary for easier access.
        """
        validation_params = {}
        try:
            # This method needs to be implemented in the datatype repository
            datatype_data_list = await self.datatype_repository.get_datatype_fields_data(datatype_id)
            
            for data_item in datatype_data_list:
                field_name = data_item.get("field_name")
                field_value = data_item.get("field_value")
                if field_name and field_value is not None:
                    validation_params[field_name] = field_value
                    
            logger.debug(f"Fetched validation parameters for datatype {datatype_id}: {validation_params}")
            return validation_params
        except Exception as e:
            logger.error(f"Error fetching validation parameters for datatype {datatype_id}: {e}")
            return {}
        
    def _validate_range(self, values: List[Any], validation_params: Dict) -> Tuple[bool, List[bool], List[str]]:
        """
        Check if values are within the range defined in validation parameters.
        
        Args:
            values: The list of values to validate
            validation_params: Dictionary of validation parameters
            
        Returns:
            Tuple of (all_valid, validation_results, error_messages)
        """
        if not values:
            return False, [], ["No values provided for range validation"]
        
        # Get min and max values from validation params
        min_value = validation_params.get("min_value")
        max_value = validation_params.get("max_value")
        
        # If no range is defined, consider all values valid
        if min_value is None or max_value is None:
            return True, [True] * len(values), []
        
        # Try to convert range bounds to float
        try:
            min_val = float(min_value)
            max_val = float(max_value)
        except (ValueError, TypeError):
            logger.warning(f"Invalid range bounds: min={min_value}, max={max_value}")
            return False, [False] * len(values), [f"Invalid range bounds: min={min_value}, max={max_value}"]
        
        # Validate each value
        all_valid = True
        validation_results = []
        error_messages = []
        
        for i, value in enumerate(values):
            if value is None:
                validation_results.append(False)
                error_messages.append(f"Value at index {i} is None")
                all_valid = False
                continue
            
            try:
                value_float = float(value)
                valid = min_val <= value_float <= max_val
                validation_results.append(valid)
                
                if not valid:
                    error_messages.append(f"Value {value} at index {i} is outside allowed range [{min_val}, {max_val}]")
                    all_valid = False
            except (ValueError, TypeError):
                validation_results.append(False)
                error_messages.append(f"Range validation failed due to type conversion error: {value}")
                all_valid = False
        
        return all_valid, validation_results, error_messages
        
    def _validate_type(self, values: List[Any], expected_type: str) -> Tuple[bool, List[Any], List[str]]:
        """
        Validate and convert a list of values to the expected type if possible.
        
        Args:
            values: The list of values to validate and convert
            expected_type: The expected data type (string, float, integer, boolean)
            
        Returns:
            Tuple of (all_valid, converted_values, error_messages)
        """
        if not values:
            return False, [], ["No values provided for validation"]
        
        if not expected_type:
            # If no expected type is specified, consider any values valid
            return True, values, []
        
        converted_values = []
        error_messages = []
        all_valid = True
        
        for i, value in enumerate(values):
            if value is None:
                converted_values.append(None)
                error_messages.append(f"Value at index {i} is None")
                all_valid = False
                continue
            
            try:
                if expected_type.lower() == 'float':
                    converted_values.append(float(value))
                elif expected_type.lower() == 'integer':
                    converted_values.append(int(float(value)))
                elif expected_type.lower() == 'boolean':
                    if isinstance(value, bool):
                        converted_values.append(value)
                    elif isinstance(value, str):
                        converted_values.append(value.lower() in ('true', 'yes', '1'))
                    else:
                        converted_values.append(bool(value))
                elif expected_type.lower() == 'string':
                    converted_values.append(str(value))
                else:
                    # Default case - no conversion
                    logger.info(f"Using default validation for unrecognized type: {expected_type}")
                    converted_values.append(value)
            except (ValueError, TypeError) as e:
                logger.warning(f"Type validation failed: {value} is not a valid {expected_type}. Error: {e}")
                converted_values.append(value)  # Keep original value
                error_messages.append(f"Type validation failed for value '{value}': expected {expected_type}. Error: {e}")
                all_valid = False
        
        return all_valid, converted_values, error_messages
               
    def _allow_partial_validation(self, validation_params: Dict) -> bool:
        """
        Determine if partial validation is allowed based on configuration.
        
        Args:
            validation_params: Dictionary of validation parameters
            
        Returns:
            True if partial validation is allowed, False otherwise
        """
        # Check for a specific flag in validation parameters
        partial_validation = validation_params.get("Validation partielle", "false")
        return partial_validation.lower() in ('true', 'yes', '1')
            
    def _build_standardized_output(
        self, 
        device_id: str, 
        datatype: Any, 
        converted_values: List[Any], 
        validation_params: Dict,
        request_id: Optional[str] = None,
        timestamp: Optional[datetime] = None
    ) -> StandardizedOutput:
        """
        Build a StandardizedOutput object from validated data.
        Also creates a ValidatedOutput object for internal validation tracking.
        
        Args:
            device_id: The device ID
            datatype: The datatype model
            converted_values: List of validated and converted values
            validation_params: Dictionary of validation parameters
            request_id: Optional request ID
            timestamp: Optional timestamp
            
        Returns:
            StandardizedOutput object with validated data
        """
        if timestamp is None:
            timestamp = datetime.utcnow()
        
        # Apply scaling to each value
        scaled_values = [self._apply_scaling(value, validation_params) for value in converted_values]
        
        # Get unit label from datatype if available
        label = []
        if hasattr(datatype, 'unit') and datatype.unit:
            label.append(datatype.unit)
        
        # Build metadata with validation info
        metadata = {
            "datatype_id": str(datatype.id) if hasattr(datatype, 'id') else None,
            "datatype_name": datatype.name if hasattr(datatype, 'name') else None,
            "validation_timestamp": datetime.utcnow().isoformat(),
            "validation_status": "valid"
        }
        
        # Add original unit if conversion was applied
        if validation_params.get("Original unit"):
            metadata["original_unit"] = validation_params.get("Original unit")
        
        # Create ValidatedOutput for internal tracking (no metadata)
        validated_output = ValidatedOutput(
            device_id=device_id,
            values=scaled_values,
            label=label,
            index=datatype.datatype_index if hasattr(datatype, 'datatype_index') else "",
            request_id=request_id,
            timestamp=timestamp
        )
        
        # Create StandardizedOutput for external output (includes metadata)
        standardized_output = StandardizedOutput(
            device_id=device_id,
            values=scaled_values,
            label=label,
            index=datatype.datatype_index if hasattr(datatype, 'datatype_index') else "",
            metadata=metadata,
            request_id=request_id,
            timestamp=timestamp
        )
        
        logger.debug(f"Created standardized output with values: {scaled_values} and metadata: {metadata}")
        return standardized_output
        
    def _apply_scaling(self, value: Any, validation_params: Dict) -> Any:
        """
        Apply scaling to the value based on validation parameters.
        
        Args:
            value: The value to scale
            validation_params: Dictionary of validation parameters
            
        Returns:
            Scaled value
        """
        if value is None:
            return None
        
        scaling_factor = validation_params.get("Facteur d'échelle")
        
        if scaling_factor is not None:
            try:
                factor = float(scaling_factor)
                return float(value) * factor
            except (ValueError, TypeError):
                logger.warning(f"Failed to apply scaling factor: {scaling_factor} to value: {value}")
                
        return value 