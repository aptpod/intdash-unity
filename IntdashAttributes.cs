using System;
using UnityEngine;
using System.Collections.Generic;
using System.Linq;

#if UNITY_EDITOR
using UnityEditor;
#endif

[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, AllowMultiple = false)]
public class IntdashLabelAttribute : PropertyAttribute
{
    public readonly string DisplayName;

    public IntdashLabelAttribute(string displayName)
    {
        DisplayName = displayName;
    }
}

#if UNITY_EDITOR
[CustomPropertyDrawer(typeof(IntdashLabelAttribute))]
public class IscpLabelDrawer : PropertyDrawer
{
    public override void OnGUI(Rect position, SerializedProperty property, GUIContent label)
    {
        var newLabel = attribute as IntdashLabelAttribute;
        EditorGUI.PropertyField(position, property, new GUIContent(newLabel.DisplayName), true);
    }

    public override float GetPropertyHeight(SerializedProperty property, GUIContent label)
    {
        return EditorGUI.GetPropertyHeight(property, true);
    }
}
#endif

[AttributeUsage(AttributeTargets.Field, AllowMultiple = false)]
public class IntdashEnumItemAttribute : Attribute
{
    public string DisplayName { get; private set; }

    public IntdashEnumItemAttribute(string displayName)
    {
        DisplayName = displayName;
    }
}

[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, AllowMultiple = false)]
public class IntdashEnumAttribute : PropertyAttribute
{
    public Type Type { get; private set; }

    public IntdashEnumAttribute(Type selfType)
    {
        Type = selfType;
    }
}

#if UNITY_EDITOR
[CustomPropertyDrawer(typeof(IntdashEnumAttribute))]
public class IntdashEnumItemDrawer : PropertyDrawer
{
    public override void OnGUI(Rect position, SerializedProperty property, GUIContent label)
    {
        var attr = attribute as IntdashEnumAttribute;
        var names = new List<string>();
        foreach (var fi in attr.Type.GetFields())
        {
            if (fi.IsSpecialName)
            {
                continue;
            }
            var elementAttribute = fi.GetCustomAttributes(typeof(IntdashEnumItemAttribute), false).FirstOrDefault() as IntdashEnumItemAttribute;
            names.Add(elementAttribute == null ? fi.Name : elementAttribute.DisplayName);
        }
        var values = Enum.GetValues(attr.Type).Cast<int>();
        property.intValue = EditorGUI.IntPopup(position, property.displayName, property.intValue, names.ToArray(), values.ToArray());
    }
}
#endif

#if UNITY_EDITOR

[CustomPropertyDrawer(typeof(IntdashVisiblityAttribute))]
internal sealed class IntdashVisiblityPropertyDrawer : PropertyDrawer
{
    public override void OnGUI(Rect position, SerializedProperty property, GUIContent label)
    {
        var attr = base.attribute as IntdashVisiblityAttribute;

        object value = null;
        var prop = property.serializedObject.FindProperty(attr.VariableName);
        if (prop == null)
        {
            Debug.LogError($"Not found '{attr.VariableName}' property");
            EditorGUI.PropertyField(position, property, label, true);
            EditorGUI.EndDisabledGroup();
            return;
        }

        var targetObject = prop.serializedObject.targetObject;
        var type = targetObject.GetType();
        var field = type.GetField(prop.propertyPath);
        value = field.GetValue(targetObject);
        var enable = value.Equals(attr.EnableValue);
        if (attr.Invisible && !enable)
        {
            return;
        }
        EditorGUI.BeginDisabledGroup(!enable);
        EditorGUI.PropertyField(position, property, label, true);
        EditorGUI.EndDisabledGroup();
    }

    public override float GetPropertyHeight(SerializedProperty property, GUIContent label)
    {
        var attr = base.attribute as IntdashVisiblityAttribute;

        var prop = property.serializedObject.FindProperty(attr.VariableName);
        if (prop == null)
        {
            return EditorGUI.GetPropertyHeight(property, true);
        }
        var targetObject = prop.serializedObject.targetObject;
        var type = targetObject.GetType();
        var field = type.GetField(prop.propertyPath);
        var value = field.GetValue(targetObject);

        var enable = value.Equals(attr.EnableValue);
        if (attr.Invisible && !enable)
        {
            return -EditorGUIUtility.standardVerticalSpacing;
        }
        return EditorGUI.GetPropertyHeight(property, true);
    }

    private bool IsEnable(IntdashVisiblityAttribute attr, object enableValue)
    {
        return attr.EnableValue == enableValue;
    }
}

#endif

public class IntdashVisiblityAttribute : PropertyAttribute
{
    public readonly string VariableName;
    public readonly object EnableValue;
    public readonly bool Disable;
    public readonly bool Invisible;

    public IntdashVisiblityAttribute(string variableName, object enableValue, bool disable = false, bool invisible = false)
    {
        this.VariableName = variableName;
        this.EnableValue = enableValue;
        this.Disable = disable;
        this.Invisible = invisible;
    }
}