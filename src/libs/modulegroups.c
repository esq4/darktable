/*
    This file is part of darktable,
    Copyright (C) 2011-2020 darktable developers.

    darktable is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    darktable is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with darktable.  If not, see <http://www.gnu.org/licenses/>.
*/

#include "bauhaus/bauhaus.h"
#include "common/darktable.h"
#include "common/debug.h"
#include "common/image_cache.h"
#include "common/iop_group.h"
#include "control/conf.h"
#include "control/control.h"
#include "develop/develop.h"
#include "dtgtk/button.h"
#include "dtgtk/icon.h"
#include "gui/accelerators.h"
#include "gui/gtk.h"
#include "libs/lib.h"
#include "libs/lib_api.h"
#ifdef GDK_WINDOWING_QUARTZ
#include "osx/osx.h"
#endif

DT_MODULE(1)

// the T_ macros are for the translation engine to take them into account
#define FALLBACK_PRESET_NAME     "modules: default"
#define T_FALLBACK_PRESET_NAME _("modules: default")

#define DEPRECATED_PRESET_NAME     "modules: deprecated"
#define T_DEPRECATED_PRESET_NAME _("modules: deprecated")

#define CURRENT_PRESET_NAME "last modified layout"
#define T_CURRENT_PRESET_NAME _("last modified layout")

// list of recommended basics widgets
#define RECOMMENDED_BASICS                                                                                        \
  "|exposure/exposure|temperature/temperature|temperature/tint|colorbalance/contrast|colorbalance/output "        \
  "saturation|clipping/angle|denoiseprofile|lens|bilat|"

// if a preset cannot be loaded or the current preset deleted, this is the fallabck preset

#define PADDING 2
#define DT_IOP_ORDER_INFO (darktable.unmuted & DT_DEBUG_IOPORDER)

#include "modulegroups.h"

typedef enum dt_lib_modulegroups_basic_item_parent_t
{
  NONE = 0,
  BOX,
  GRID
} dt_lib_modulegroups_basic_item_parent_t;

typedef enum dt_lib_modulegroups_basic_item_type_t
{
  WIDGET_TYPE_NONE = 0,
  WIDGET_TYPE_BAUHAUS_SLIDER,
  WIDGET_TYPE_BAUHAUS_COMBO,
  WIDGET_TYPE_ACTIVATE_BTN,
  WIDGET_TYPE_MISC
} dt_lib_modulegroups_basic_item_type_t;

typedef struct dt_lib_modulegroups_basic_item_t
{
  gchar *id;
  gchar *module_op;
  gchar *widget_name; // translated
  GtkWidget *widget;
  GtkWidget *temp_widget;
  GtkWidget *old_parent;
  dt_lib_modulegroups_basic_item_parent_t old_parent_type;
  dt_lib_modulegroups_basic_item_type_t widget_type;

  int old_pos;
  gboolean expand;
  gboolean fill;
  guint padding;
  GtkPackType packtype;
  gboolean sensitive;
  gchar *tooltip;
  gchar *label;
  gboolean visible;
  int grid_x, grid_y, grid_w, grid_h;


  GtkWidget *box;
  dt_iop_module_t *module;
} dt_lib_modulegroups_basic_item_t;

typedef struct dt_lib_modulegroups_group_t
{
  gchar *name;
  GtkWidget *button;
  gchar *icon;
  GtkWidget *iop_box;
  // default
  GList *modules;
} dt_lib_modulegroups_group_t;

typedef struct dt_lib_modulegroups_t
{
  uint32_t current;
  GtkWidget *text_entry;
  GtkWidget *hbox_buttons;
  GtkWidget *active_btn;
  GtkWidget *basic_btn;
  GtkWidget *hbox_groups;
  GtkWidget *hbox_search_box;
  GtkWidget *deprecated;

  GList *groups;
  gboolean show_search;

  GList *edit_groups;
  gboolean edit_show_search;
  gchar *edit_preset;
  gboolean edit_ro;
  gboolean edit_basics_show;
  GList *edit_basics;

  // editor dialog
  GtkWidget *dialog;
  GtkWidget *presets_list, *preset_box;
  GtkWidget *preset_name, *preset_groups_box;
  GtkWidget *edit_search_cb;
  GtkWidget *basics_chkbox, *edit_basics_groupbox, *edit_basics_box;

  gboolean basics_show;
  GList *basics;
  GtkWidget *vbox_basic;
} dt_lib_modulegroups_t;

typedef enum dt_lib_modulegroup_iop_visibility_type_t
{
  DT_MODULEGROUP_SEARCH_IOP_TEXT_VISIBLE,
  DT_MODULEGROUP_SEARCH_IOP_GROUPS_VISIBLE,
  DT_MODULEGROUP_SEARCH_IOP_TEXT_GROUPS_VISIBLE
} dt_lib_modulegroup_iop_visibility_type_t;

/* toggle button callback */
static void _lib_modulegroups_toggle(GtkWidget *button, gpointer data);
/* helper function to update iop module view depending on group */
static void _lib_modulegroups_update_iop_visibility(dt_lib_module_t *self);

/* modulergroups proxy set group function
   \see dt_dev_modulegroups_set()
*/
static void _lib_modulegroups_set(dt_lib_module_t *self, uint32_t group);
/* modulegroups proxy update visibility function
*/
static void _lib_modulegroups_update_visibility_proxy(dt_lib_module_t *self);
/* modulegroups proxy get group function
  \see dt_dev_modulegroups_get()
*/
static uint32_t _lib_modulegroups_get(dt_lib_module_t *self);
/* modulegroups proxy test function.
   tests if iop module group flags matches modulegroup.
*/
static gboolean _lib_modulegroups_test(dt_lib_module_t *self, uint32_t group, dt_iop_module_t *module);
/* modulegroups proxy test visibility function.
   tests if iop module is preset in one groups for current layout.
*/
static gboolean _lib_modulegroups_test_visible(dt_lib_module_t *self, gchar *module);
/* modulegroups proxy switch group function.
   sets the active group which module belongs too.
*/
static void _lib_modulegroups_switch_group(dt_lib_module_t *self, dt_iop_module_t *module);
/* modulergroups proxy search text focus function
   \see dt_dev_modulegroups_search_text_focus()
*/
static void _lib_modulegroups_search_text_focus(dt_lib_module_t *self);

static void _manage_preset_update_list(dt_lib_module_t *self);
static void _manage_editor_load(const char *preset, dt_lib_module_t *self);

static void _buttons_update(dt_lib_module_t *self);

const char *name(dt_lib_module_t *self)
{
  return _("modulegroups");
}

const char **views(dt_lib_module_t *self)
{
  static const char *v[] = {"darkroom", NULL};
  return v;
}

uint32_t container(dt_lib_module_t *self)
{
  return DT_UI_CONTAINER_PANEL_RIGHT_TOP;
}


/* this module should always be shown without expander */
int expandable(dt_lib_module_t *self)
{
  return 0;
}

int position()
{
  return 999;
}

static GtkWidget *_buttons_get_from_pos(dt_lib_module_t *self, const int pos)
{
  dt_lib_modulegroups_t *d = (dt_lib_modulegroups_t *)self->data;
  if(pos == DT_MODULEGROUP_ACTIVE_PIPE) return d->active_btn;
  if(pos == DT_MODULEGROUP_BASICS) return d->basic_btn;
  dt_lib_modulegroups_group_t *gr = (dt_lib_modulegroups_group_t *)g_list_nth_data(d->groups, pos - 1);
  if(gr) return gr->button;
  return NULL;
}

static void _text_entry_changed_callback(GtkEntry *entry, dt_lib_module_t *self)
{
  _lib_modulegroups_update_iop_visibility(self);
}

static gboolean _text_entry_icon_press_callback(GtkEntry *entry, GtkEntryIconPosition icon_pos, GdkEvent *event,
                                                dt_lib_module_t *self)
{
  dt_lib_modulegroups_t *d = (dt_lib_modulegroups_t *)self->data;

  gtk_entry_set_text(GTK_ENTRY(d->text_entry), "");

  return TRUE;
}

static gboolean _text_entry_key_press_callback(GtkWidget *widget, GdkEventKey *event, gpointer user_data)
{

  if(event->keyval == GDK_KEY_Escape)
  {
    gtk_entry_set_text(GTK_ENTRY(widget), "");
    gtk_widget_grab_focus(dt_ui_center(darktable.gui->ui));
    return TRUE;
  }

  return FALSE;
}

static DTGTKCairoPaintIconFunc _buttons_get_icon_fct(gchar *icon)
{
  if(g_strcmp0(icon, "active") == 0)
    return dtgtk_cairo_paint_modulegroup_active;
  else if(g_strcmp0(icon, "favorites") == 0)
    return dtgtk_cairo_paint_modulegroup_favorites;
  else if(g_strcmp0(icon, "tone") == 0)
    return dtgtk_cairo_paint_modulegroup_tone;
  else if(g_strcmp0(icon, "color") == 0)
    return dtgtk_cairo_paint_modulegroup_color;
  else if(g_strcmp0(icon, "correct") == 0)
    return dtgtk_cairo_paint_modulegroup_correct;
  else if(g_strcmp0(icon, "effect") == 0)
    return dtgtk_cairo_paint_modulegroup_effect;
  else if(g_strcmp0(icon, "grading") == 0)
    return dtgtk_cairo_paint_modulegroup_grading;
  else if(g_strcmp0(icon, "technical") == 0)
    return dtgtk_cairo_paint_modulegroup_technical;

  return dtgtk_cairo_paint_modulegroup_basic;
}

void gui_cleanup(dt_lib_module_t *self)
{
  dt_lib_modulegroups_t *d = (dt_lib_modulegroups_t *)self->data;

  dt_gui_key_accel_block_on_focus_disconnect(d->text_entry);

  darktable.develop->proxy.modulegroups.module = NULL;
  darktable.develop->proxy.modulegroups.set = NULL;
  darktable.develop->proxy.modulegroups.get = NULL;
  darktable.develop->proxy.modulegroups.test = NULL;
  darktable.develop->proxy.modulegroups.switch_group = NULL;

  g_free(self->data);
  self->data = NULL;
}

static gint _iop_compare(gconstpointer a, gconstpointer b)
{
  return g_strcmp0((gchar *)a, (gchar *)b);
}
static gboolean _lib_modulegroups_test_internal(dt_lib_module_t *self, uint32_t group, dt_iop_module_t *module)
{
  if(group == DT_MODULEGROUP_ACTIVE_PIPE) return module->enabled;
  dt_lib_modulegroups_t *d = (dt_lib_modulegroups_t *)self->data;
  dt_lib_modulegroups_group_t *gr = (dt_lib_modulegroups_group_t *)g_list_nth_data(d->groups, group - 1);
  if(gr)
  {
    return (g_list_find_custom(gr->modules, module->so->op, _iop_compare) != NULL);
  }
  return FALSE;
}

static gboolean _lib_modulegroups_test(dt_lib_module_t *self, uint32_t group, dt_iop_module_t *module)
{
  return _lib_modulegroups_test_internal(self, group, module);
}

static gboolean _lib_modulegroups_test_visible(dt_lib_module_t *self, gchar *module)
{
  dt_lib_modulegroups_t *d = (dt_lib_modulegroups_t *)self->data;
  GList *l = d->groups;
  while(l)
  {
    dt_lib_modulegroups_group_t *gr = (dt_lib_modulegroups_group_t *)l->data;
    if(g_list_find_custom(gr->modules, module, _iop_compare) != NULL)
    {
      return TRUE;
    }
    l = g_list_next(l);
  }
  return FALSE;
}

// initialize item names, ...
static void _basics_get_names_from_accel_path(gchar *path, char **id, char **module_op, gchar **widget_name)
{
  // path are in the form : <Darktable>/image operations/IMAGE_OP[/WIDGET/NAME]/dynamic
  gchar **elems = g_strsplit(path, "/", -1);
  if(g_strv_length(elems) > 3)
  {
    if(id)
    {
      if(g_strv_length(elems) > 5)
        *id = dt_util_dstrcat(NULL, "%s/%s/%s", elems[2], elems[3], elems[4]);
      else if(g_strv_length(elems) > 4)
        *id = dt_util_dstrcat(NULL, "%s/%s", elems[2], elems[3]);
      else
        *id = g_strdup(elems[2]);
    }
    if(module_op) *module_op = g_strdup(elems[2]);
    if(widget_name)
    {
      if(g_strv_length(elems) > 5)
        *widget_name = dt_util_dstrcat(NULL, "%s - %s", _(elems[3]), _(elems[4]));
      else if(g_strv_length(elems) > 4)
        *widget_name = dt_util_dstrcat(NULL, "%s", _(elems[3]));
      else
        *widget_name = g_strdup(_("on-off"));
    }
  }
  g_strfreev(elems);
}
static void _basics_init_item(dt_lib_modulegroups_basic_item_t *item)
{
  if(!item->id) return;

  gchar **elems = g_strsplit(item->id, "/", -1);
  if(g_strv_length(elems) > 0)
  {
    item->module_op = g_strdup(elems[0]);
    if(g_strv_length(elems) > 2)
      item->widget_name = dt_util_dstrcat(NULL, "%s - %s", _(elems[1]), _(elems[2]));
    else if(g_strv_length(elems) > 1)
      item->widget_name = dt_util_dstrcat(NULL, "%s", _(elems[1]));
    else
    {
      item->widget_name = g_strdup(_("on-off"));
      item->widget_type = WIDGET_TYPE_ACTIVATE_BTN;
    }
  }
  g_strfreev(elems);
}

static void _basics_free_item(dt_lib_modulegroups_basic_item_t *item)
{
  g_free(item->id);
  g_free(item->module_op);
  if(item->tooltip) g_free(item->tooltip);
  g_free(item->widget_name);
}

static void _basics_remove_widget(dt_lib_modulegroups_basic_item_t *item)
{
  if(item->widget && item->widget_type != WIDGET_TYPE_ACTIVATE_BTN)
  {
    // put back the widget in its iop at the right place
    if(GTK_IS_CONTAINER(item->old_parent) && gtk_widget_get_parent(item->widget) == item->box)
    {
      g_object_ref(item->widget);
      gtk_container_remove(GTK_CONTAINER(gtk_widget_get_parent(item->widget)), item->widget);

      if(item->old_parent_type == BOX)
      {
        if(item->packtype == GTK_PACK_START)
          gtk_box_pack_start(GTK_BOX(item->old_parent), item->widget, item->expand, item->fill, item->padding);
        else
          gtk_box_pack_end(GTK_BOX(item->old_parent), item->widget, item->expand, item->fill, item->padding);

        gtk_box_reorder_child(GTK_BOX(item->old_parent), item->widget, item->old_pos);
      }
      else if(item->old_parent_type == GRID)
      {
        gtk_grid_attach(GTK_GRID(item->old_parent), item->widget, item->grid_x, item->grid_y, item->grid_w,
                        item->grid_h);
      }

      g_object_unref(item->widget);
    }
    // put back sensitivity, visibility and tooltip
    if(GTK_IS_WIDGET(item->widget))
    {
      gtk_widget_set_sensitive(item->widget, item->sensitive);
      gtk_widget_set_visible(item->widget, item->visible);
      gtk_widget_set_tooltip_text(item->widget, item->tooltip);
    }
    // put back label
    if(item->label && DT_IS_BAUHAUS_WIDGET(item->widget))
    {
      DtBauhausWidget *bw = DT_BAUHAUS_WIDGET(item->widget);
      snprintf(bw->label, sizeof(bw->label), "%s", item->label);
    }
  }
  // cleanup item
  if(item->box) gtk_widget_destroy(item->box);
  if(item->temp_widget) gtk_widget_destroy(item->temp_widget);
  item->box = NULL;
  item->temp_widget = NULL;
  item->widget = NULL;
  item->old_parent = NULL;
  item->module = NULL;
  if(item->tooltip)
  {
    g_free(item->tooltip);
    item->tooltip = NULL;
  }
  if(item->label)
  {
    g_free(item->label);
    item->label = NULL;
  }
}

static void _basics_hide(dt_lib_module_t *self)
{
  dt_lib_modulegroups_t *d = (dt_lib_modulegroups_t *)self->data;
  if(!d->vbox_basic) return;
  gtk_widget_hide(d->vbox_basic);

  GList *l = d->basics;
  while(l)
  {
    dt_lib_modulegroups_basic_item_t *item = (dt_lib_modulegroups_basic_item_t *)l->data;
    _basics_remove_widget(item);
    l = g_list_next(l);
  }
  gtk_widget_destroy(d->vbox_basic);
  d->vbox_basic = NULL;
}

static gboolean _basics_goto_module(GtkWidget *w, GdkEventButton *e, gpointer user_data)
{
  dt_iop_module_t *module = (dt_iop_module_t *)(user_data);
  dt_dev_modulegroups_switch(darktable.develop, module);
  dt_iop_gui_set_expanded(module, TRUE, TRUE);
  dt_iop_gui_set_expanded(module, TRUE, FALSE);
  return TRUE;
}

static void _basics_on_off_callback(GtkWidget *btn, dt_lib_modulegroups_basic_item_t *item)
{
  // we switch the "real" button accordingly
  if(darktable.gui->reset) return;
  gtk_toggle_button_set_active(GTK_TOGGLE_BUTTON(item->widget),
                               gtk_toggle_button_get_active(GTK_TOGGLE_BUTTON(btn)));
}
static void _basics_on_off_callback2(GtkWidget *widget, GdkEventButton *e, dt_lib_modulegroups_basic_item_t *item)
{
  // we get the button and change its state
  GtkToggleButton *btn
      = (GtkToggleButton *)g_list_nth_data(gtk_container_get_children(GTK_CONTAINER(item->box)), 0);
  if(btn)
  {
    darktable.gui->reset++;
    gtk_toggle_button_set_active(btn, !gtk_toggle_button_get_active(btn));
    darktable.gui->reset--;
    gtk_toggle_button_toggled(btn);
  }
}

static void _basics_add_widget(dt_lib_module_t *self, dt_lib_modulegroups_basic_item_t *item, DtBauhausWidget *bw,
                               gboolean new_group)
{
  dt_lib_modulegroups_t *d = (dt_lib_modulegroups_t *)self->data;

  // if widget already exists, let's remove it and read it correctly
  if(item->widget)
  {
    _basics_remove_widget(item);
    if(item->widget) return; // we shouldn't arrive here !
  }

  // we retrieve parents, positions, etc... so we can put the widget back in its module
  if(item->widget_type == WIDGET_TYPE_ACTIVATE_BTN)
  {
    // on-off widgets
    item->widget = GTK_WIDGET(item->module->off);
    item->sensitive = gtk_widget_get_sensitive(item->widget);
    item->tooltip = g_strdup(gtk_widget_get_tooltip_text(item->widget));

    // create new basic widget
    item->box = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 0);
    gtk_widget_set_name(item->box, "basics-widget");

    // we create a new button linked with the real one
    // because it create too much pb to remove the button from the expander
    GtkWidget *btn
        = dtgtk_togglebutton_new(dtgtk_cairo_paint_switch, CPF_STYLE_FLAT | CPF_BG_TRANSPARENT, item->module);
    gtk_widget_set_name(btn, "module-enable-button");
    gtk_toggle_button_set_active(GTK_TOGGLE_BUTTON(btn),
                                 gtk_toggle_button_get_active(GTK_TOGGLE_BUTTON(item->widget)));
    g_signal_connect(G_OBJECT(btn), "toggled", G_CALLBACK(_basics_on_off_callback), item);
    gtk_box_pack_start(GTK_BOX(item->box), btn, FALSE, FALSE, 0);
    GtkWidget *evb = gtk_event_box_new();
    GtkWidget *lb = gtk_label_new(item->module->name());
    gtk_label_set_xalign(GTK_LABEL(lb), 0.0);
    gtk_widget_set_name(lb, "basics-iop_name");
    gtk_container_add(GTK_CONTAINER(evb), lb);
    g_signal_connect(G_OBJECT(evb), "button-press-event", G_CALLBACK(_basics_on_off_callback2), item);
    gtk_box_pack_start(GTK_BOX(item->box), evb, FALSE, TRUE, 0);

    // disable widget if needed (multiinstance)
    if(dt_iop_count_instances(item->module->so) > 1)
    {
      gtk_widget_set_sensitive(evb, FALSE);
      gtk_widget_set_sensitive(btn, FALSE);
      gtk_widget_set_tooltip_text(lb, _("This basic widget is disabled as there's multiple instances "
                                        "for this module. You need to use the full module..."));
      gtk_widget_set_tooltip_text(btn, _("This basic widget is disabled as there's multiple instances "
                                         "for this module. You need to use the full module..."));
    }
    else
    {
      GtkWidget *orig_label = (GtkWidget *)g_list_nth_data(
          gtk_container_get_children(GTK_CONTAINER(item->module->header)), IOP_MODULE_LABEL);
      gtk_widget_set_tooltip_text(lb, gtk_widget_get_tooltip_text(orig_label));
      gtk_widget_set_tooltip_text(btn, gtk_widget_get_tooltip_text(orig_label));
    }

    gtk_widget_show_all(item->box);
  }
  else
  {
    // classic widgets (sliders and combobox)
    if(!bw || !GTK_IS_WIDGET(bw)) return;
    GtkWidget *w = GTK_WIDGET(bw);

    if(GTK_IS_BOX(gtk_widget_get_parent(w)))
    {
      item->old_parent_type = BOX;
      item->widget = w;
      item->module = bw->module;
      item->old_parent = gtk_widget_get_parent(item->widget);
      // we retrieve current positions, etc...
      gtk_box_query_child_packing(GTK_BOX(item->old_parent), item->widget, &item->expand, &item->fill,
                                  &item->padding, &item->packtype);
      GValue v = G_VALUE_INIT;
      g_value_init(&v, G_TYPE_INT);
      gtk_container_child_get_property(GTK_CONTAINER(item->old_parent), item->widget, "position", &v);
      item->old_pos = g_value_get_int(&v);
    }
    else if(GTK_IS_GRID(gtk_widget_get_parent(w)))
    {
      item->old_parent_type = GRID;
      item->widget = w;
      item->module = bw->module;
      item->old_parent = gtk_widget_get_parent(item->widget);

      GValue v = G_VALUE_INIT;
      g_value_init(&v, G_TYPE_INT);
      gtk_container_child_get_property(GTK_CONTAINER(item->old_parent), item->widget, "left-attach", &v);
      item->grid_x = g_value_get_int(&v);
      gtk_container_child_get_property(GTK_CONTAINER(item->old_parent), item->widget, "top-attach", &v);
      item->grid_y = g_value_get_int(&v);
      gtk_container_child_get_property(GTK_CONTAINER(item->old_parent), item->widget, "width", &v);
      item->grid_w = g_value_get_int(&v);
      gtk_container_child_get_property(GTK_CONTAINER(item->old_parent), item->widget, "height", &v);
      item->grid_h = g_value_get_int(&v);
    }
    else
    {
      // we don't allow other parents at the moment
      item->old_parent_type = NONE;
      return;
    }

    // save old values
    item->sensitive = gtk_widget_get_sensitive(item->widget);
    item->tooltip = g_strdup(gtk_widget_get_tooltip_text(item->widget));
    item->label = g_strdup(bw->label);
    item->visible = gtk_widget_get_visible(item->widget);

    // create new basic widget
    item->box = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 0);
    gtk_widget_set_name(item->box, "basics-widget");
    gtk_widget_show(item->box);

    // we reparent the iop widget here
    g_object_ref(item->widget);
    gtk_container_remove(GTK_CONTAINER(item->old_parent), item->widget);
    gtk_box_pack_start(GTK_BOX(item->box), item->widget, TRUE, TRUE, 0);
    g_object_unref(item->widget);

    // change the widget label to integrate section name
    snprintf(bw->label, sizeof(bw->label), "%s", item->widget_name);

    // we put the temporary widget at the place of the real widget in the module
    // this avoid order mismatch when putting back the real widget
    item->temp_widget = gtk_label_new("temp widget");
    if(GTK_IS_CONTAINER(item->old_parent))
    {
      if(item->old_parent_type == BOX)
      {
        if(item->packtype == GTK_PACK_START)
          gtk_box_pack_start(GTK_BOX(item->old_parent), item->temp_widget, item->expand, item->fill, item->padding);
        else
          gtk_box_pack_end(GTK_BOX(item->old_parent), item->temp_widget, item->expand, item->fill, item->padding);

        gtk_box_reorder_child(GTK_BOX(item->old_parent), item->temp_widget, item->old_pos);
      }
      else if(item->old_parent_type == GRID)
      {
        gtk_grid_attach(GTK_GRID(item->old_parent), item->temp_widget, item->grid_x, item->grid_y, item->grid_w,
                        item->grid_h);
      }
    }

    // disable widget if needed (multiinstance)
    if(dt_iop_count_instances(item->module->so) > 1)
    {
      gtk_widget_set_sensitive(item->widget, FALSE);
      gtk_widget_set_tooltip_text(item->widget, _("This basic widget is disabled as there's multiple instances "
                                                  "for this module. You need to use the full module..."));
    }
    else if(!item->visible)
    {
      gtk_widget_show_all(item->widget);
      gtk_widget_set_sensitive(item->widget, FALSE);
      gtk_widget_set_tooltip_text(item->widget, _("This basic widget is disabled as it's hidden in the actual "
                                                  "module configuration. You need to use the full module..."));
    }
    else
    {
      gchar *txt = dt_util_dstrcat(NULL, "%s (%s)\n\n%s\n\n%s", item->widget_name, item->module->name(),
                                   item->tooltip, _("(some features may only be available in the full module)"));
      gtk_widget_set_tooltip_text(item->widget, txt);
      g_free(txt);
    }
  }

  // if it's the first widget of a module, we want to show a separation
  if(new_group)
  {
    if(dt_conf_get_bool("plugins/darkroom/modulegroups_basics_sections_labels"))
    {
      // we add the section label
      GtkWidget *sect = dt_ui_section_label_new(item->module->name());
      gtk_label_set_xalign(GTK_LABEL(sect), 0.5); // we center the module name
      gtk_box_pack_start(GTK_BOX(d->vbox_basic), sect, FALSE, FALSE, 0);
      gtk_widget_show_all(sect);
    }
    else
    {
      // we just add a thin line on top of the widget to show delimitation
      GtkStyleContext *context = gtk_widget_get_style_context(item->box);
      gtk_style_context_add_class(context, "basics-widget_group_start");
    }
  }

  // and we add the link to the full iop
  GtkWidget *wbt = dtgtk_button_new(dtgtk_cairo_paint_preferences, CPF_STYLE_FLAT | CPF_DO_NOT_USE_BORDER, NULL);
  gchar *tt = dt_util_dstrcat(NULL, _("go to full version of module %s"), item->module->name());
  gtk_widget_set_tooltip_text(wbt, tt);
  gtk_widget_set_name(wbt, "basics-link");
  g_free(tt);
  g_signal_connect(G_OBJECT(wbt), "button-press-event", G_CALLBACK(_basics_goto_module), item->module);
  gtk_box_pack_end(GTK_BOX(item->box), wbt, FALSE, FALSE, 0);
  gtk_widget_show(wbt);

  gtk_box_pack_start(GTK_BOX(d->vbox_basic), item->box, FALSE, FALSE, 0);
}

static void _basics_show(dt_lib_module_t *self)
{
  dt_lib_modulegroups_t *d = (dt_lib_modulegroups_t *)self->data;

  if(d->vbox_basic && gtk_widget_get_visible(d->vbox_basic)) return;

  if(!d->vbox_basic)
  {
    d->vbox_basic = gtk_box_new(GTK_ORIENTATION_VERTICAL, 0);
    dt_ui_container_add_widget(darktable.gui->ui, DT_UI_CONTAINER_PANEL_RIGHT_CENTER, d->vbox_basic);
  }
  if(dt_conf_get_bool("plugins/darkroom/modulegroups_basics_sections_labels"))
    gtk_widget_set_name(d->vbox_basic, "basics-box-labels");
  else
    gtk_widget_set_name(d->vbox_basic, "basics-box");

  int pos = 0;
  GList *modules = g_list_last(darktable.develop->iop);
  while(modules)
  {
    dt_iop_module_t *module = (dt_iop_module_t *)(modules->data);
    gboolean new_module = TRUE;      // we record if it's a new module or not to set css class
    if(pos == 0 && !dt_conf_get_bool("plugins/darkroom/modulegroups_basics_sections_labels"))
      new_module = FALSE; // except for the first one as we don't want top separator
    if(!dt_iop_is_hidden(module) && !(module->flags() & IOP_FLAGS_DEPRECATED) && module->iop_order != INT_MAX)
    {
      // first, we add on-off buttons if any
      GList *l = d->basics;
      while(l)
      {
        dt_lib_modulegroups_basic_item_t *item = (dt_lib_modulegroups_basic_item_t *)l->data;
        if(!item->module && g_strcmp0(item->module_op, module->op) == 0)
        {
          if(item->widget_type == WIDGET_TYPE_ACTIVATE_BTN)
          {
            item->module = module;
            _basics_add_widget(self, item, NULL, new_module);
            new_module = FALSE;
            pos++;
          }
        }
        l = g_list_next(l);
      }

      // and we add all other widgets
      gchar *pre = dt_util_dstrcat(NULL, "<Darktable>/image operations/%s/", module->op);
      GList *la = g_list_last(darktable.control->accelerator_list);
      while(la)
      {
        dt_accel_t *accel = (dt_accel_t *)la->data;
        if(accel && accel->closure && accel->closure->data && g_str_has_prefix(accel->path, pre)
           && g_str_has_suffix(accel->path, "/dynamic") && DT_IS_BAUHAUS_WIDGET(accel->closure->data))
        {
          DtBauhausWidget *ww = DT_BAUHAUS_WIDGET(accel->closure->data);
          if(ww->module == module)
          {
            l = d->basics;
            while(l)
            {
              dt_lib_modulegroups_basic_item_t *item = (dt_lib_modulegroups_basic_item_t *)l->data;
              if(!item->module && g_strcmp0(item->module_op, module->op) == 0
                 && item->widget_type != WIDGET_TYPE_ACTIVATE_BTN)
              {
                gchar *tx = dt_util_dstrcat(NULL, "<Darktable>/image operations/%s/dynamic", item->id);
                if(!strcmp(accel->path, tx))
                {
                  item->module = module;
                  _basics_add_widget(self, item, ww, new_module);
                  new_module = FALSE;
                  pos++;
                  g_free(tx);
                  break;
                }
                g_free(tx);
              }
              l = g_list_next(l);
            }
          }
        }
        la = g_list_previous(la);
      }
      g_free(pre);
    }
    modules = g_list_previous(modules);
  }

  gtk_widget_show(d->vbox_basic);
}

static void _lib_modulegroups_update_iop_visibility(dt_lib_module_t *self)
{
  dt_lib_modulegroups_t *d = (dt_lib_modulegroups_t *)self->data;

  // we hide eventual basic panel
  if(d->current == DT_MODULEGROUP_BASICS && !d->basics_show) d->current = DT_MODULEGROUP_ACTIVE_PIPE;
  _basics_hide(self);

  const gchar *text_entered = (gtk_widget_is_visible(GTK_WIDGET(d->hbox_search_box)))
                                  ? gtk_entry_get_text(GTK_ENTRY(d->text_entry))
                                  : NULL;

  if (DT_IOP_ORDER_INFO)
    fprintf(stderr,"\n^^^^^ modulegroups");

  /* only show module group as selected if not currently searching */
  if(d->show_search && d->current != DT_MODULEGROUP_NONE)
  {
    GtkWidget *bt = _buttons_get_from_pos(self, d->current);
    if(bt)
    {
      /* toggle button visibility without executing callback */
      g_signal_handlers_block_matched(bt, G_SIGNAL_MATCH_FUNC, 0, 0, NULL, _lib_modulegroups_toggle, NULL);

      if(text_entered && text_entered[0] != '\0')
        gtk_toggle_button_set_active(GTK_TOGGLE_BUTTON(bt), FALSE);
      else
        gtk_toggle_button_set_active(GTK_TOGGLE_BUTTON(bt), TRUE);

      g_signal_handlers_unblock_matched(bt, G_SIGNAL_MATCH_FUNC, 0, 0, NULL, _lib_modulegroups_toggle, NULL);
    }
  }

  // hide deprectade message. it will be shown after if needed
  gtk_widget_set_visible(d->deprecated, FALSE);

  GList *modules = darktable.develop->iop;
  if(modules)
  {
    /*
     * iterate over ip modules and do various test to
     * detect if the modules should be shown or not.
     */
    do
    {
      dt_iop_module_t *module = (dt_iop_module_t *)modules->data;
      GtkWidget *w = module->expander;

      if ((DT_IOP_ORDER_INFO) && (module->enabled))
      {
        fprintf(stderr,"\n%20s %d",module->op, module->iop_order);
        if(dt_iop_is_hidden(module)) fprintf(stderr,", hidden");
      }

      /* skip modules without an gui */
      if(dt_iop_is_hidden(module)) continue;

      // do not show non-active modules
      // we don't want the user to mess with those
      if(module->iop_order == INT_MAX)
      {
        if(darktable.develop->gui_module == module) dt_iop_request_focus(NULL);
        if(w) gtk_widget_hide(w);
        continue;
      }

      // if there's some search text show matching modules only
      if(text_entered && text_entered[0] != '\0')
      {
        /* don't show deprecated ones unless they are enabled */
        if(module->flags() & IOP_FLAGS_DEPRECATED && !(module->enabled))
        {
          if(darktable.develop->gui_module == module) dt_iop_request_focus(NULL);
          if(w) gtk_widget_hide(w);
        }
        else
        {
           const int is_match = (g_strstr_len(g_utf8_casefold(dt_iop_get_localized_name(module->op), -1), -1,
                                              g_utf8_casefold(text_entered, -1))
                                 != NULL) ||
                                 (g_strstr_len(g_utf8_casefold(dt_iop_get_localized_aliases(module->op), -1), -1,
                                               g_utf8_casefold(text_entered, -1))
                                 != NULL);


          if(is_match)
            gtk_widget_show(w);
          else
            gtk_widget_hide(w);
        }
        continue;
      }

      /* lets show/hide modules dependent on current group*/
      const gboolean show_deprecated
          = !strcmp(dt_conf_get_string("plugins/darkroom/modulegroups_preset"), _(DEPRECATED_PRESET_NAME));
      switch(d->current)
      {
        case DT_MODULEGROUP_BASICS:
        {
          if(darktable.develop->gui_module == module) dt_iop_request_focus(NULL);
          if(w) gtk_widget_hide(w);
        }
        break;

        case DT_MODULEGROUP_ACTIVE_PIPE:
        {
          if(module->enabled)
          {
            if(w) gtk_widget_show(w);
          }
          else
          {
            if(darktable.develop->gui_module == module) dt_iop_request_focus(NULL);
            if(w) gtk_widget_hide(w);
          }
        }
        break;

        case DT_MODULEGROUP_NONE:
        {
          /* show all except hidden ones */
          if(((!(module->flags() & IOP_FLAGS_DEPRECATED) || show_deprecated)
              && _lib_modulegroups_test_visible(self, module->op))
             || module->enabled)
          {
            if(w) gtk_widget_show(w);
          }
          else
          {
            if(darktable.develop->gui_module == module) dt_iop_request_focus(NULL);
            if(w) gtk_widget_hide(w);
          }
        }
        break;

        default:
        {
          // show deprecated module in specific group deprecated
          gtk_widget_set_visible(d->deprecated, show_deprecated);

          if(_lib_modulegroups_test_internal(self, d->current, module)
             && (!(module->flags() & IOP_FLAGS_DEPRECATED) || module->enabled || show_deprecated))
          {
            if(w) gtk_widget_show(w);
          }
          else
          {
            if(darktable.develop->gui_module == module) dt_iop_request_focus(NULL);
            if(w) gtk_widget_hide(w);
          }
        }
      }
    } while((modules = g_list_next(modules)) != NULL);
  }
  if (DT_IOP_ORDER_INFO) fprintf(stderr,"\nvvvvv\n");
  // now that visibility has been updated set multi-show
  dt_dev_modules_update_multishow(darktable.develop);

  // we show eventual basic panel
  if(d->current == DT_MODULEGROUP_BASICS) _basics_show(self);
}

static void _lib_modulegroups_toggle(GtkWidget *button, gpointer user_data)
{
  dt_lib_module_t *self = (dt_lib_module_t *)user_data;
  dt_lib_modulegroups_t *d = (dt_lib_modulegroups_t *)self->data;
  const gchar *text_entered = (gtk_widget_is_visible(GTK_WIDGET(d->hbox_search_box)))
                                  ? gtk_entry_get_text(GTK_ENTRY(d->text_entry))
                                  : NULL;

  /* block all button callbacks */
  for(int k = 0; k <= g_list_length(d->groups); k++)
    g_signal_handlers_block_matched(_buttons_get_from_pos(self, k), G_SIGNAL_MATCH_FUNC, 0, 0, NULL,
                                    _lib_modulegroups_toggle, NULL);
  g_signal_handlers_block_matched(d->basic_btn, G_SIGNAL_MATCH_FUNC, 0, 0, NULL, _lib_modulegroups_toggle, NULL);

  /* deactivate all buttons */
  int gid = 0;
  for(int k = 0; k <= g_list_length(d->groups); k++)
  {
    const GtkWidget *bt = _buttons_get_from_pos(self, k);
    /* store toggled modulegroup */
    if(bt == button) gid = k;
    gtk_toggle_button_set_active(GTK_TOGGLE_BUTTON(bt), FALSE);
  }
  if(button == d->basic_btn) gid = DT_MODULEGROUP_BASICS;
  gtk_toggle_button_set_active(GTK_TOGGLE_BUTTON(d->basic_btn), FALSE);

  /* only deselect button if not currently searching else re-enable module */
  if(d->current == gid && !(text_entered && text_entered[0] != '\0'))
    d->current = DT_MODULEGROUP_NONE;
  else
  {
    d->current = gid;
    gtk_toggle_button_set_active(GTK_TOGGLE_BUTTON(_buttons_get_from_pos(self, gid)), TRUE);
  }

  /* unblock all button callbacks */
  for(int k = 0; k <= g_list_length(d->groups); k++)
    g_signal_handlers_unblock_matched(_buttons_get_from_pos(self, k), G_SIGNAL_MATCH_FUNC, 0, 0, NULL,
                                      _lib_modulegroups_toggle, NULL);
  g_signal_handlers_unblock_matched(d->basic_btn, G_SIGNAL_MATCH_FUNC, 0, 0, NULL, _lib_modulegroups_toggle, NULL);

  /* clear search text */
  if(gtk_widget_is_visible(GTK_WIDGET(d->hbox_search_box)))
  {
    g_signal_handlers_block_matched(d->text_entry, G_SIGNAL_MATCH_FUNC, 0, 0, NULL, _text_entry_changed_callback, NULL);
    gtk_entry_set_text(GTK_ENTRY(d->text_entry), "");
    g_signal_handlers_unblock_matched(d->text_entry, G_SIGNAL_MATCH_FUNC, 0, 0, NULL, _text_entry_changed_callback, NULL);
  }

  /* update visibility */
  _lib_modulegroups_update_iop_visibility(self);
}

typedef struct _set_gui_thread_t
{
  dt_lib_module_t *self;
  uint32_t group;
} _set_gui_thread_t;

static gboolean _lib_modulegroups_set_gui_thread(gpointer user_data)
{
  _set_gui_thread_t *params = (_set_gui_thread_t *)user_data;

  /* set current group and update visibility */
  GtkWidget *bt = _buttons_get_from_pos(params->self, params->group);
  if(bt) gtk_toggle_button_set_active(GTK_TOGGLE_BUTTON(bt), TRUE);
  _lib_modulegroups_update_iop_visibility(params->self);

  free(params);
  return FALSE;
}

static gboolean _lib_modulegroups_upd_gui_thread(gpointer user_data)
{
  _set_gui_thread_t *params = (_set_gui_thread_t *)user_data;

  _lib_modulegroups_update_iop_visibility(params->self);

  free(params);
  return FALSE;
}

static gboolean _lib_modulegroups_search_text_focus_gui_thread(gpointer user_data)
{
  _set_gui_thread_t *params = (_set_gui_thread_t *)user_data;

  dt_lib_modulegroups_t *d = (dt_lib_modulegroups_t *)params->self->data;

  if(GTK_IS_ENTRY(d->text_entry))
  {
    if(!gtk_widget_is_visible(GTK_WIDGET(d->hbox_search_box))) gtk_widget_show(GTK_WIDGET(d->hbox_search_box));
    gtk_widget_grab_focus(GTK_WIDGET(d->text_entry));
  }

  free(params);
  return FALSE;
}

/* this is a proxy function so it might be called from another thread */
static void _lib_modulegroups_set(dt_lib_module_t *self, uint32_t group)
{
  _set_gui_thread_t *params = (_set_gui_thread_t *)malloc(sizeof(_set_gui_thread_t));
  if(!params) return;
  params->self = self;
  params->group = group;
  g_main_context_invoke(NULL, _lib_modulegroups_set_gui_thread, params);
}

/* this is a proxy function so it might be called from another thread */
static void _lib_modulegroups_update_visibility_proxy(dt_lib_module_t *self)
{
  _set_gui_thread_t *params = (_set_gui_thread_t *)malloc(sizeof(_set_gui_thread_t));
  if(!params) return;
  params->self = self;
  g_main_context_invoke(NULL, _lib_modulegroups_upd_gui_thread, params);
}

/* this is a proxy function so it might be called from another thread */
static void _lib_modulegroups_search_text_focus(dt_lib_module_t *self)
{
  _set_gui_thread_t *params = (_set_gui_thread_t *)malloc(sizeof(_set_gui_thread_t));
  if(!params) return;
  params->self = self;
  params->group = 0;
  g_main_context_invoke(NULL, _lib_modulegroups_search_text_focus_gui_thread, params);
}

static void _lib_modulegroups_switch_group(dt_lib_module_t *self, dt_iop_module_t *module)
{
  /* lets find the group which is not active pipe */
  dt_lib_modulegroups_t *d = (dt_lib_modulegroups_t *)self->data;
  for(int k = 1; k <= g_list_length(d->groups); k++)
  {
    if(_lib_modulegroups_test(self, k, module))
    {
      _lib_modulegroups_set(self, k);
      return;
    }
  }
}

static uint32_t _lib_modulegroups_get(dt_lib_module_t *self)
{
  dt_lib_modulegroups_t *d = (dt_lib_modulegroups_t *)self->data;
  return d->current;
}

static dt_lib_modulegroup_iop_visibility_type_t _preset_retrieve_old_search_pref(gchar **ret)
{
  // show the search box ?
  gchar *show_text_entry = dt_conf_get_string("plugins/darkroom/search_iop_by_text");
  dt_lib_modulegroup_iop_visibility_type_t val = DT_MODULEGROUP_SEARCH_IOP_TEXT_GROUPS_VISIBLE;

  if(strcmp(show_text_entry, "show search text") == 0)
  {
    // we only show the search box. no groups
    *ret = dt_util_dstrcat(*ret, "1ꬹ1");
    val = DT_MODULEGROUP_SEARCH_IOP_TEXT_VISIBLE;
  }
  else if(strcmp(show_text_entry, "show groups") == 0)
  {
    // we don't show the search box
    *ret = dt_util_dstrcat(*ret, "0");
    val = DT_MODULEGROUP_SEARCH_IOP_GROUPS_VISIBLE;
  }
  else
  {
    // we show both
    *ret = dt_util_dstrcat(*ret, "1");
    val = DT_MODULEGROUP_SEARCH_IOP_TEXT_GROUPS_VISIBLE;
  }
  g_free(show_text_entry);
  return val;
}

/* presets syntax :
  Layout presets are saved as string consisting of blocs separated by ꬹ
  OPTIONSꬹBLOC_0ꬹBLOC_1ꬹBLOC_2....
  OPTION : just show_search(0-1)
  BLOC_0 : reserved for future use. Always 1
  BLOC_1.... : blocs describing each group, contains :
    name|icon_name||iop_name_0|iop_name_1....
*/

static gchar *_preset_retrieve_old_layout_updated()
{
  gchar *ret = NULL;

  // show the search box ?
  if(_preset_retrieve_old_search_pref(&ret) == DT_MODULEGROUP_SEARCH_IOP_TEXT_VISIBLE) return ret;

  // layout with "new" 3 groups
  for(int i = 0; i < 4; i++)
  {
    // group name and icon
    if(i == 0)
    {
      ret = dt_util_dstrcat(ret, "ꬹ1|||%s",
                            "exposure/exposure|temperature/temperature|temperature/tint|colorbalance/contrast"
                            "|colorbalance/output saturation|clipping/angle|denoiseprofile|lens|bilat");
      ret = dt_util_dstrcat(ret, "ꬹfavorites|favorites|");
    }
    else if(i == 1)
      ret = dt_util_dstrcat(ret, "ꬹtechnical|technical|");
    else if(i == 2)
      ret = dt_util_dstrcat(ret, "ꬹgrading|grading|");
    else if(i == 3)
      ret = dt_util_dstrcat(ret, "ꬹeffects|effect|");

    // list of modules
    GList *modules = g_list_first(darktable.iop);
    while(modules)
    {
      dt_iop_module_so_t *module = (dt_iop_module_so_t *)(modules->data);

      if(!dt_iop_so_is_hidden(module) && !(module->flags() & IOP_FLAGS_DEPRECATED))
      {
        // get previous visibility values
        const int group = module->default_group();
        gchar *key = dt_util_dstrcat(NULL, "plugins/darkroom/%s/visible", module->op);
        const gboolean visi = dt_conf_get_bool(key);
        g_free(key);
        key = dt_util_dstrcat(NULL, "plugins/darkroom/%s/favorite", module->op);
        const gboolean fav = dt_conf_get_bool(key);
        g_free(key);

        if((i == 0 && fav && visi) || (i == 1 && (group & IOP_GROUP_TECHNICAL) && visi)
           || (i == 2 && (group & IOP_GROUP_GRADING) && visi) || (i == 3 && (group & IOP_GROUP_EFFECTS) && visi))
        {
          ret = dt_util_dstrcat(ret, "|%s", module->op);
        }
      }
      modules = g_list_next(modules);
    }
  }
  return ret;
}

static gchar *_preset_retrieve_old_layout(char *list, char *list_fav)
{
  gchar *ret = NULL;

  // show the search box ?
  if(_preset_retrieve_old_search_pref(&ret) == DT_MODULEGROUP_SEARCH_IOP_TEXT_VISIBLE) return ret;

  // layout with "old" 5 groups
  for(int i = 0; i < 6; i++)
  {
    // group name and icon
    if(i == 0)
    {
      // we don't have to care about "modern" worflow for temperature as it's more recent than this layout
      ret = dt_util_dstrcat(ret, "ꬹ1|||%s",
                            "exposure/exposure|temperature/temperature|temperature/tint|colorbalance/contrast"
                            "|colorbalance/output saturation|clipping/angle|denoiseprofile|lens|bilat");
      ret = dt_util_dstrcat(ret, "ꬹfavorites|favorites|");
    }
    else if(i == 1)
      ret = dt_util_dstrcat(ret, "ꬹbase|basic|");
    else if(i == 2)
      ret = dt_util_dstrcat(ret, "ꬹtone|tone|");
    else if(i == 3)
      ret = dt_util_dstrcat(ret, "ꬹcolor|color|");
    else if(i == 4)
      ret = dt_util_dstrcat(ret, "ꬹcorrect|correct|");
    else if(i == 5)
      ret = dt_util_dstrcat(ret, "ꬹeffect|effect|");

    // list of modules
    GList *modules = g_list_first(darktable.iop);
    while(modules)
    {
      dt_iop_module_so_t *module = (dt_iop_module_so_t *)(modules->data);

      if(!dt_iop_so_is_hidden(module) && !(module->flags() & IOP_FLAGS_DEPRECATED))
      {
        gchar *search = dt_util_dstrcat(NULL, "|%s|", module->op);
        gchar *key;

        // get previous visibility values
        int group = -1;
        if(i > 0 && list)
        {
          // we retrieve the group from hardcoded one
          const int gr = module->default_group();
          if(gr & IOP_GROUP_BASIC)
            group = 1;
          else if(gr & IOP_GROUP_TONE)
            group = 2;
          else if(gr & IOP_GROUP_COLOR)
            group = 3;
          else if(gr & IOP_GROUP_CORRECT)
            group = 4;
          else if(gr & IOP_GROUP_EFFECT)
            group = 5;
        }
        else if(i > 0)
        {
          key = dt_util_dstrcat(NULL, "plugins/darkroom/%s/modulegroup", module->op);
          group = dt_conf_get_int(key);
          g_free(key);
        }

        gboolean visi = FALSE;
        if(list)
          visi = (strstr(list, search) != NULL);
        else
        {
          key = dt_util_dstrcat(NULL, "plugins/darkroom/%s/visible", module->op);
          visi = dt_conf_get_bool(key);
          g_free(key);
        }

        gboolean fav = FALSE;
        if(i == 0 && list_fav)
          fav = (strstr(list_fav, search) != NULL);
        else if(i == 0)
        {
          key = dt_util_dstrcat(NULL, "plugins/darkroom/%s/favorite", module->op);
          fav = dt_conf_get_bool(key);
          g_free(key);
        }

        if((i == 0 && fav && visi) || (i == group && visi))
        {
          ret = dt_util_dstrcat(ret, "|%s", module->op);
        }

        g_free(search);
      }
      modules = g_list_next(modules);
    }
  }
  return ret;
}

static void _preset_retrieve_old_presets(dt_lib_module_t *self)
{
  // we retrieve old modulelist presets
  sqlite3_stmt *stmt;
  DT_DEBUG_SQLITE3_PREPARE_V2(dt_database_get(darktable.db),
                              "SELECT name, op_params"
                              " FROM data.presets"
                              " WHERE operation = 'modulelist' AND op_version = 1 AND writeprotect = 0",
                              -1, &stmt, NULL);

  while(sqlite3_step(stmt) == SQLITE_ROW)
  {
    const char *pname = (char *)sqlite3_column_text(stmt, 0);
    const char *p = (char *)sqlite3_column_blob(stmt, 1);
    const int size = sqlite3_column_bytes(stmt, 1);

    gchar *list = NULL;
    gchar *fav = NULL;
    int pos = 0;
    while(pos < size)
    {
      const char *op = p + pos;
      int op_len = strlen(op);
      dt_iop_module_state_t state = p[pos + op_len + 1];

      if(state == dt_iop_state_ACTIVE)
        list = dt_util_dstrcat(list, "|%s", op);
      else if(state == dt_iop_state_FAVORITE)
      {
        fav = dt_util_dstrcat(fav, "|%s", op);
        list = dt_util_dstrcat(list, "|%s", op);
      }
      pos += op_len + 2;
    }
    list = dt_util_dstrcat(list, "|");
    fav = dt_util_dstrcat(fav, "|");

    gchar *tx = _preset_retrieve_old_layout(list, fav);
    dt_lib_presets_add(pname, self->plugin_name, self->version(), tx, strlen(tx), FALSE);
    g_free(tx);
  }
  sqlite3_finalize(stmt);

  // and we remove all existing modulelist presets
  DT_DEBUG_SQLITE3_EXEC(dt_database_get(darktable.db),
                        "DELETE FROM data.presets"
                        " WHERE operation = 'modulelist' AND op_version = 1",
                        NULL, NULL, NULL);
}

static gchar *_preset_to_string(dt_lib_module_t *self, gboolean edition)
{
  dt_lib_modulegroups_t *d = (dt_lib_modulegroups_t *)self->data;
  gchar *res = NULL;
  const gboolean show_search = edition ? d->edit_show_search : d->show_search;
  res = dt_util_dstrcat(res, "%d", show_search ? 1 : 0);

  const gboolean basics_show = edition ? d->edit_basics_show : d->basics_show;
  GList *basics = edition ? d->edit_basics : d->basics;
  GList *groups = edition ? d->edit_groups : d->groups;

  // basics widgets
  res = dt_util_dstrcat(res, "ꬹ%d||", basics_show ? 1 : 0);
  GList *l = basics;
  while(l)
  {
    dt_lib_modulegroups_basic_item_t *item = (dt_lib_modulegroups_basic_item_t *)l->data;
    res = dt_util_dstrcat(res, "|%s", item->id);
    l = g_list_next(l);
  }

  l = groups;
  while(l)
  {
    dt_lib_modulegroups_group_t *g = (dt_lib_modulegroups_group_t *)l->data;
    res = dt_util_dstrcat(res, "ꬹ%s|%s|", g->name, g->icon);
    GList *ll = g->modules;
    while(ll)
    {
      gchar *m = (gchar *)ll->data;
      res = dt_util_dstrcat(res, "|%s", m);
      ll = g_list_next(ll);
    }
    l = g_list_next(l);
  }

  return res;
}

static void _preset_from_string(dt_lib_module_t *self, gchar *txt, gboolean edition)
{
  if(!txt) return;
  dt_lib_modulegroups_t *d = (dt_lib_modulegroups_t *)self->data;

  GList *res = NULL;
  gboolean show_search = TRUE;

  gchar **gr = g_strsplit(txt, "ꬹ", -1);

  // read the general options
  if(g_strv_length(gr) > 0)
  {
    // we just have show_search for instance
    if(!g_strcmp0(gr[0], "0")) show_search = FALSE;
  }

  // read the basics widgets
  if(g_strv_length(gr) > 1)
  {
    if(gr[1])
    {
      gchar **gr2 = g_strsplit(gr[1], "|", -1);
      gboolean basics_show = FALSE;
      if(g_strv_length(gr2) > 3 && (g_strcmp0(gr2[0], "1") == 0)) basics_show = TRUE;
      if(edition)
        d->edit_basics_show = basics_show;
      else
        d->basics_show = basics_show;

      for(int j = 3; j < g_strv_length(gr2); j++)
      {
        dt_lib_modulegroups_basic_item_t *item
            = (dt_lib_modulegroups_basic_item_t *)g_malloc0(sizeof(dt_lib_modulegroups_basic_item_t));
        item->id = g_strdup(gr2[j]);
        _basics_init_item(item);

        if(edition)
          d->edit_basics = g_list_append(d->edit_basics, item);
        else
          d->basics = g_list_append(d->basics, item);
      }
      g_strfreev(gr2);
    }
  }

  // read the groups
  for(int i = 2; i < g_strv_length(gr); i++)
  {
    gchar *tx = gr[i];
    if(tx)
    {
      gchar **gr2 = g_strsplit(tx, "|", -1);
      const int nb = g_strv_length(gr2);
      if(nb > 2)
      {
        dt_lib_modulegroups_group_t *group
            = (dt_lib_modulegroups_group_t *)g_malloc0(sizeof(dt_lib_modulegroups_group_t));
        group->name = g_strdup(gr2[0]);
        group->icon = g_strdup(gr2[1]);
        // gr2[2] is reserved for eventual future use
        for(int j = 3; j < nb; j++)
        {
          group->modules = g_list_append(group->modules, g_strdup(gr2[j]));
        }
        res = g_list_append(res, group);
      }
      g_strfreev(gr2);
    }
  }
  g_strfreev(gr);

  // and we set the values
  if(edition)
  {
    d->edit_show_search = show_search;
    d->edit_groups = res;
  }
  else
  {
    d->show_search = show_search;
    d->groups = res;
  }
}

void init_presets(dt_lib_module_t *self)
{
  /*
    For the record, one can create the preset list by using the following code:

    $ cat <( git grep "return.*IOP_GROUP_TONE" -- src/iop/ | cut -d':' -f1 ) \
          <( git grep IOP_FLAGS_DEPRECATED -- src/iop/ | cut -d':' -f1 ) | \
          grep -E -v "useless|mask_manager|gamma" | sort | uniq --unique | \
          while read file; do BN=$(basename $(basename $file .cc) .c); \
            echo ${BN:0:16} ; done | xargs echo | sed 's/ /|/g'
  */

  // we define here specific sequences which depends of user prefs
  gchar *basic_temp = NULL;
  if(g_strcmp0(dt_conf_get_string("plugins/darkroom/chromatic-adaptation"), "modern") == 0)
    basic_temp = dt_util_dstrcat(NULL, "channelmixerrgb/temperature");
  else
    basic_temp = dt_util_dstrcat(NULL, "temperature/temperature|temperature/tint");

  // all modules
  gchar *tx = NULL;
  tx = dt_util_dstrcat(tx, "ꬹ1|||%s|%s", basic_temp,
                       "exposure/exposure|colorbalance/contrast"
                       "|colorbalance/output saturation|clipping/angle|denoiseprofile|lens|bilat");
  tx = dt_util_dstrcat(tx, "ꬹ%s|%s||%s", C_("modulegroup", "base"), "basic",
                       "basecurve|basicadj|clipping|colisa|colorreconstruct|demosaic|exposure|finalscale"
                       "|flip|highlights|negadoctor|overexposed|rawoverexposed|rawprepare"
                       "|shadhi|temperature|toneequal");
  tx = dt_util_dstrcat(tx, "ꬹ%s|%s||%s", C_("modulegroup", "tone"), "tone",
                       "bilat|filmicrgb|levels"
                       "|rgbcurve|rgblevels|tonecurve");
  tx = dt_util_dstrcat(tx, "ꬹ%s|%s||%s", C_("modulegroup", "color"), "color",
                       "channelmixerrgb|colorbalance|colorchecker|colorcontrast"
                       "|colorcorrection|colorin|colorout|colorzones|lut3d|monochrome"
                       "|profile_gamma|velvia|vibrance");
  tx = dt_util_dstrcat(tx, "ꬹ%s|%s||%s", C_("modulegroup", "correct"), "correct",
                       "ashift|atrous|bilateral|cacorrect|defringe|denoiseprofile|dither"
                       "|hazeremoval|hotpixels|lens|liquify|nlmeans|rawdenoise|retouch|rotatepixels"
                       "|scalepixels|sharpen|spots");
  tx = dt_util_dstrcat(tx, "ꬹ%s|%s||%s", C_("modulegroup", "effect"), "effect",
                       "bloom|borders|colorize|colormapping|graduatednd|grain|highpass|lowlight"
                       "|lowpass|soften|splittoning|vignette|watermark");
  dt_lib_presets_add(_("modules: all"), self->plugin_name, self->version(), tx, strlen(tx), TRUE);
  g_free(tx);

  // minimal / 3 tabs
  tx = NULL;
  tx = dt_util_dstrcat(tx, "ꬹ1|||%s|%s", basic_temp, "exposure/exposure|clipping/angle|denoiseprofile|lens");
  tx = dt_util_dstrcat(tx, "ꬹ%s|%s||%s", C_("modulegroup", "base"), "basic",
                       "basicadj|ashift|basecurve|clipping"
                       "|denoiseprofile|exposure|flip|lens|temperature");
  tx = dt_util_dstrcat(tx, "ꬹ%s|%s||%s", C_("modulegroup", "grading"), "grading",
                       "channelmixerrgb|colorzones|graduatednd|rgbcurve"
                       "|rgblevels|splittoning");
  tx = dt_util_dstrcat(tx, "ꬹ%s|%s||%s", C_("modulegroup", "effects"), "effect",
                       "borders|monochrome|retouch|sharpen|vignette|watermark");
  dt_lib_presets_add(_("workflow: beginner"), self->plugin_name, self->version(), tx, strlen(tx), TRUE);
  g_free(tx);

  // display referred
  tx = NULL;
  tx = dt_util_dstrcat(tx, "ꬹ1|||%s|%s", basic_temp,
                       "exposure/exposure|colorbalance/contrast"
                       "|colorbalance/output saturation|clipping/angle|denoiseprofile|lens|bilat");
  tx = dt_util_dstrcat(tx, "ꬹ%s|%s||%s", C_("modulegroup", "base"), "basic",
                       "basecurve|toneequal|clipping|flip|exposure|temperature"
                       "|rgbcurve|rgblevels|bilat|shadhi|highlights");
  tx = dt_util_dstrcat(tx, "ꬹ%s|%s||%s", C_("modulegroup", "color"), "color",
                       "channelmixerrgb|colorbalance|colorcorrection|colorzones|monochrome|velvia|vibrance");
  tx = dt_util_dstrcat(tx, "ꬹ%s|%s||%s", C_("modulegroup", "correct"), "correct",
                       "ashift|cacorrect|defringe|denoiseprofile|hazeremoval|hotpixels"
                       "|lens|retouch|liquify|sharpen|nlmeans");
  tx = dt_util_dstrcat(tx, "ꬹ%s|%s||%s", C_("modulegroup", "effect"), "effect",
                       "borders|colorize|graduatednd|grain|splittoning|vignette|watermark");
  dt_lib_presets_add(_("workflow: display-referred"), self->plugin_name, self->version(), tx, strlen(tx), TRUE);
  g_free(tx);

  // scene referred
  tx = NULL;
  tx = dt_util_dstrcat(tx, "ꬹ1|||%s|%s", basic_temp,
                       "exposure/exposure|colorbalance/contrast"
                       "|colorbalance/output saturation|clipping/angle|denoiseprofile|lens|bilat");
  tx = dt_util_dstrcat(tx, "ꬹ%s|%s||%s", C_("modulegroup", "base"), "basic",
                       "filmicrgb|toneequal|clipping|flip|exposure|temperature|bilat");
  tx = dt_util_dstrcat(tx, "ꬹ%s|%s||%s", C_("modulegroup", "color"), "color",
                       "channelmixerrgb|colorbalance|colorzones");
  tx = dt_util_dstrcat(tx, "ꬹ%s|%s||%s", C_("modulegroup", "correct"), "correct",
                       "ashift|cacorrect|defringe|denoiseprofile|hazeremoval|hotpixels"
                       "|lens|retouch|liquify|sharpen|nlmeans");
  tx = dt_util_dstrcat(tx, "ꬹ%s|%s||%s", C_("modulegroup", "effect"), "effect",
                       "atrous|borders|graduatednd|grain|vignette|watermark");
  dt_lib_presets_add(_("workflow: scene-referred"), self->plugin_name, self->version(), tx, strlen(tx), TRUE);
  g_free(tx);

  // default / 3 tabs based on Aurélien's proposal
  tx = NULL;
  tx = dt_util_dstrcat(tx, "ꬹ1|||%s|%s", basic_temp,
                       "exposure/exposure|colorbalance/contrast"
                       "|colorbalance/output saturation|clipping/angle|denoiseprofile|lens|bilat");
  tx = dt_util_dstrcat(tx, "ꬹ%s|%s||%s", C_("modulegroup", "technical"), "technical",
                       "ashift|basecurve|bilateral|cacorrect|clipping|colorchecker|colorin|colorout"
                       "|colorreconstruct|defringe|demosaic|denoiseprofile|dither|exposure"
                       "|filmicrgb|finalscale|flip|hazeremoval|highlights|hotpixels|lens"
                       "|lut3d|negadoctor|nlmeans|overexposed|rawdenoise"
                       "|rawoverexposed|rotatepixels||temperature|scalepixels");
  tx = dt_util_dstrcat(tx, "ꬹ%s|%s||%s", C_("modulegroup", "grading"), "grading",
                       "basicadj|channelmixerrgb|colisa|colorbalance"
                       "|colorcontrast|colorcorrection|colorize|colorzones"
                       "|graduatednd|levels|rgbcurve|rgblevels|shadhi|splittoning"
                       "|tonecurve|toneequal"
                       "|velvia|vibrance");
  tx = dt_util_dstrcat(tx, "ꬹ%s|%s||%s", C_("modulegroup", "effects"), "effect",
                       "atrous|bilat|bloom|borders|colormapping"
                       "|grain|highpass|liquify|lowlight|lowpass|monochrome|retouch|sharpen"
                       "|soften|spots|vignette|watermark");
  dt_lib_presets_add(_(FALLBACK_PRESET_NAME), self->plugin_name, self->version(), tx, strlen(tx), TRUE);
  g_free(tx);

  // search only (only active modules visibles)
  tx = NULL;
  tx = dt_util_dstrcat(tx, "1ꬹ1");
  dt_lib_presets_add(_("search only"), self->plugin_name, self->version(), tx, strlen(tx), TRUE);
  g_free(tx);

  // this is a special preset for all newly deprecated modules
  // so users still have a chance to access them until next release (with warning messages)
  // this modules are deprecated in 3.4 and should be removed from this group in 3.6
  tx = NULL;
  tx = dt_util_dstrcat(tx, "1ꬹ1ꬹ%s|%s||%s", C_("modulegroup", "deprecated"), "basic",
                       "zonesystem|invert|channelmixer|globaltonemap|relight|tonemap");
  dt_lib_presets_add(_(DEPRECATED_PRESET_NAME), self->plugin_name, self->version(), tx, strlen(tx), TRUE);
  g_free(tx);

  // if needed, we add a new preset, based on last user config
  if(!dt_conf_key_exists("plugins/darkroom/modulegroups_preset"))
  {
    tx = _preset_retrieve_old_layout(NULL, NULL);
    dt_lib_presets_add(_("previous config"), self->plugin_name, self->version(), tx, strlen(tx), FALSE);
    dt_conf_set_string("plugins/darkroom/modulegroups_preset", _("previous layout"));
    g_free(tx);

    tx = _preset_retrieve_old_layout_updated();
    dt_lib_presets_add(_("previous config with new layout"), self->plugin_name, self->version(), tx,
                       strlen(tx), FALSE);
    g_free(tx);
  }
  // if they exists, we retrieve old user presets from old modulelist lib
  _preset_retrieve_old_presets(self);
}

void *legacy_params(dt_lib_module_t *self, const void *const old_params, const size_t old_params_size,
                    const int old_version, int *new_version, size_t *new_size)
{
  return NULL;
}

void *get_params(dt_lib_module_t *self, int *size)
{
  gchar *tx = _preset_to_string(self, FALSE);
  *size = strlen(tx);
  return tx;
}

static void _manage_editor_groups_cleanup(dt_lib_module_t *self, gboolean edition)
{
  dt_lib_modulegroups_t *d = (dt_lib_modulegroups_t *)self->data;

  GList *l = edition ? d->edit_groups : d->groups;

  while(l)
  {
    dt_lib_modulegroups_group_t *gr = (dt_lib_modulegroups_group_t *)l->data;
    g_free(gr->name);
    g_free(gr->icon);
    g_list_free_full(gr->modules, g_free);
    l = g_list_next(l);
  }

  if(edition)
  {
    g_list_free_full(d->edit_groups, g_free);
    d->edit_groups = NULL;
  }
  else
  {
    g_list_free_full(d->groups, g_free);
    d->groups = NULL;
    _basics_hide(self);
  }

  l = edition ? d->edit_basics : d->basics;
  while(l)
  {
    dt_lib_modulegroups_basic_item_t *item = (dt_lib_modulegroups_basic_item_t *)l->data;
    _basics_free_item(item);
    l = g_list_next(l);
  }
  if(edition)
  {
    g_list_free_full(d->edit_basics, g_free);
    d->edit_basics = NULL;
  }
  else
  {
    g_list_free_full(d->basics, g_free);
    d->basics = NULL;
  }
}

static void _manage_editor_basics_remove(GtkWidget *widget, GdkEventButton *event, dt_lib_module_t *self)
{
  dt_lib_modulegroups_t *d = (dt_lib_modulegroups_t *)self->data;

  char *id = (char *)g_object_get_data(G_OBJECT(widget), "widget_id");
  GList *l = d->edit_basics;
  while(l)
  {
    dt_lib_modulegroups_basic_item_t *item = (dt_lib_modulegroups_basic_item_t *)l->data;
    if(g_strcmp0(item->id, id) == 0)
    {
      _basics_free_item(item);
      d->edit_basics = g_list_delete_link(d->edit_basics, l);
      gtk_widget_destroy(gtk_widget_get_parent(widget));
      break;
    }
    l = g_list_next(l);
  }
}

static int _manage_editor_module_find_multi(gconstpointer a, gconstpointer b)
{
  // we search for a other instance of module with lower priority
  dt_iop_module_t *ma = (dt_iop_module_t *)a;
  dt_iop_module_t *mb = (dt_iop_module_t *)b;
  if(g_strcmp0(ma->op, mb->op) != 0) return 1;
  if(ma->multi_priority >= mb->multi_priority) return 0;
  return 1;
}

static void _manage_editor_basics_update_list(dt_lib_module_t *self)
{
  dt_lib_modulegroups_t *d = (dt_lib_modulegroups_t *)self->data;

  // first, we remove all existing modules
  GList *lw = gtk_container_get_children(GTK_CONTAINER(d->edit_basics_box));
  while(lw)
  {
    GtkWidget *w = (GtkWidget *)lw->data;
    gtk_widget_destroy(w);
    lw = g_list_next(lw);
  }
  g_list_free(lw);

  // and we add the ones from the list
  GList *modules = g_list_last(darktable.develop->iop);
  while(modules)
  {
    dt_iop_module_t *module = (dt_iop_module_t *)(modules->data);
    GList *l = d->edit_basics;
    while(l)
    {
      dt_lib_modulegroups_basic_item_t *item = (dt_lib_modulegroups_basic_item_t *)l->data;

      if(g_strcmp0(module->op, item->module_op) == 0 && !dt_iop_is_hidden(module))
      {
        // we want to avoid showing multiple instances of the same module
        if(module->multi_priority <= 0
           || g_list_find_custom(darktable.develop->iop, module, _manage_editor_module_find_multi) == NULL)
        {
          GtkWidget *hb = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 0);
          gtk_widget_set_name(hb, "modulegroups-iop-header");
          gchar *lbn = dt_util_dstrcat(NULL, "%s\n    %s", module->name(), item->widget_name);
          GtkWidget *lb = gtk_label_new(lbn);
          g_free(lbn);
          gtk_widget_set_name(lb, "iop-panel-label");
          gtk_box_pack_start(GTK_BOX(hb), lb, FALSE, TRUE, 0);
          if(!d->edit_ro)
          {
            GtkWidget *btn = dtgtk_button_new(dtgtk_cairo_paint_cancel, CPF_STYLE_FLAT, NULL);
            gtk_widget_set_name(btn, "module-reset-button");
            gtk_widget_set_tooltip_text(btn, _("remove this widget"));
            g_object_set_data(G_OBJECT(btn), "widget_id", item->id);
            g_signal_connect(G_OBJECT(btn), "button-press-event", G_CALLBACK(_manage_editor_basics_remove), self);
            gtk_box_pack_end(GTK_BOX(hb), btn, FALSE, TRUE, 0);
          }
          gtk_box_pack_start(GTK_BOX(d->edit_basics_box), hb, FALSE, TRUE, 0);
        }
      }

      l = g_list_next(l);
    }

    modules = g_list_previous(modules);
  }

  gtk_widget_show_all(d->edit_basics_box);
}

static void _basics_cleanup_list(dt_lib_module_t *self, gboolean edition)
{
  dt_lib_modulegroups_t *d = (dt_lib_modulegroups_t *)self->data;

  // ensure here that there's no basics widget of a module not present in one other group
  GList *l = edition ? d->edit_basics : d->basics;
  while(l)
  {
    dt_lib_modulegroups_basic_item_t *item = (dt_lib_modulegroups_basic_item_t *)l->data;
    gboolean exists = FALSE;
    GList *ll = edition ? d->edit_groups : d->groups;
    while(ll)
    {
      dt_lib_modulegroups_group_t *gr = (dt_lib_modulegroups_group_t *)ll->data;
      if(g_list_find_custom(gr->modules, item->module_op, _iop_compare))
      {
        exists = TRUE;
        break;
      }
      ll = g_list_next(ll);
    }
    // if the module doesn't exists, let's remove the widget
    if(!exists)
    {
      GList *ln = g_list_next(l);
      _basics_free_item(item);
      if(edition)
        d->edit_basics = g_list_delete_link(d->edit_basics, l);
      else
        d->basics = g_list_delete_link(d->basics, l);
      l = ln;
      continue;
    }
    l = g_list_next(l);
  }
  // if we are on edition mode, we need to update the box too
  if(edition && d->edit_basics_box && GTK_IS_BOX(d->edit_basics_box)) _manage_editor_basics_update_list(self);
}

int set_params(dt_lib_module_t *self, const void *params, int size)
{
  if(!params) return 1;

  // cleanup existing groups
  _manage_editor_groups_cleanup(self, FALSE);

  _preset_from_string(self, (char *)params, FALSE);

  gchar *tx = dt_util_dstrcat(NULL, "plugins/darkroom/%s/last_preset", self->plugin_name);
  dt_conf_set_string("plugins/darkroom/modulegroups_preset", dt_conf_get_string(tx));
  g_free(tx);

  _buttons_update(self);
  return 0;
}

static void _manage_editor_save(dt_lib_module_t *self)
{
  dt_lib_modulegroups_t *d = (dt_lib_modulegroups_t *)self->data;
  if(!d->edit_groups || !d->edit_preset) return;

  // get all the values
  d->edit_show_search = gtk_toggle_button_get_active(GTK_TOGGLE_BUTTON(d->edit_search_cb));
  gchar *params = _preset_to_string(self, TRUE);
  gchar *newname = g_strdup(gtk_entry_get_text(GTK_ENTRY(d->preset_name)));

  // update the preset in the database
  dt_lib_presets_update(d->edit_preset, self->plugin_name, self->version(), newname, "", params, strlen(params));
  g_free(params);

  // if name has changed, we need to reflect the change on the presets list too
  _manage_preset_update_list(self);

  // update groups
  gchar *preset = dt_conf_get_string("plugins/darkroom/modulegroups_preset");
  if(g_strcmp0(preset, newname) == 0)
  {
    // if name has changed, let's update it
    if(g_strcmp0(d->edit_preset, newname) != 0)
      dt_conf_set_string("plugins/darkroom/modulegroups_preset", newname);
    // and we update the gui
    if(!dt_lib_presets_apply(newname, self->plugin_name, self->version()))
      dt_lib_presets_apply((gchar *)C_("modulegroup", FALLBACK_PRESET_NAME),
                           self->plugin_name, self->version());
  }
  g_free(preset);
  g_free(newname);
}

static void _manage_editor_module_remove(GtkWidget *widget, GdkEventButton *event, dt_lib_module_t *self)
{
  char *module = (char *)g_object_get_data(G_OBJECT(widget), "module_name");
  dt_lib_modulegroups_group_t *gr = (dt_lib_modulegroups_group_t *)g_object_get_data(G_OBJECT(widget), "group");

  GList *l = gr->modules;
  while(l)
  {
    const char *tx = (char *)l->data;
    if(g_strcmp0(tx, module) == 0)
    {
      g_free(l->data);
      gr->modules = g_list_delete_link(gr->modules, l);
      gtk_widget_destroy(gtk_widget_get_parent(widget));
      break;
    }
    l = g_list_next(l);
  }
  // we also remove eventual widgets of this module in basics
  _basics_cleanup_list(self, TRUE);
}

static void _manage_editor_module_update_list(dt_lib_module_t *self, dt_lib_modulegroups_group_t *gr)
{
  dt_lib_modulegroups_t *d = (dt_lib_modulegroups_t *)self->data;

  // first, we remove all existing modules
  GList *lw = gtk_container_get_children(GTK_CONTAINER(gr->iop_box));
  while(lw)
  {
    GtkWidget *w = (GtkWidget *)lw->data;
    gtk_widget_destroy(w);
    lw = g_list_next(lw);
  }

  // and we add the ones from the list
  GList *modules2 = g_list_last(darktable.develop->iop);
  while(modules2)
  {
    dt_iop_module_t *module = (dt_iop_module_t *)(modules2->data);
    if((!(module->flags() & IOP_FLAGS_DEPRECATED) || !g_strcmp0(gr->name, C_("modulegroup", "deprecated")))
       && !dt_iop_is_hidden(module) && g_list_find_custom(gr->modules, module->op, _iop_compare))
    {
      // we want to avoid showing multiple instances of the same module
      if(module->multi_priority <= 0
         || g_list_find_custom(darktable.develop->iop, module, _manage_editor_module_find_multi) == NULL)
      {
        GtkWidget *hb = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 0);
        gtk_widget_set_name(hb, "modulegroups-iop-header");
        GtkWidget *lb = gtk_label_new(module->name());
        gtk_widget_set_name(lb, "iop-panel-label");
        gtk_box_pack_start(GTK_BOX(hb), lb, FALSE, TRUE, 0);
        if(!d->edit_ro)
        {
          GtkWidget *btn = dtgtk_button_new(dtgtk_cairo_paint_cancel, CPF_STYLE_FLAT, NULL);
          gtk_widget_set_name(btn, "module-reset-button");
          gtk_widget_set_tooltip_text(btn, _("remove this module"));
          g_object_set_data(G_OBJECT(btn), "module_name", module->op);
          g_object_set_data(G_OBJECT(btn), "group", gr);
          g_signal_connect(G_OBJECT(btn), "button-press-event", G_CALLBACK(_manage_editor_module_remove), self);
          gtk_box_pack_end(GTK_BOX(hb), btn, FALSE, TRUE, 0);
        }
        gtk_box_pack_start(GTK_BOX(gr->iop_box), hb, FALSE, TRUE, 0);
      }
    }
    modules2 = g_list_previous(modules2);
  }

  gtk_widget_show_all(gr->iop_box);
}

static void _manage_editor_group_update_arrows(GtkWidget *box)
{
  // we go throw all group collumns
  GList *lw = gtk_container_get_children(GTK_CONTAINER(box));
  int pos = 0;
  const int max = g_list_length(lw) - 1;
  while(lw)
  {
    GtkWidget *w = (GtkWidget *)lw->data;
    GtkWidget *hb = (GtkWidget *)g_list_nth_data(gtk_container_get_children(GTK_CONTAINER(w)), 0);
    if(hb)
    {
      GList *lw2 = gtk_container_get_children(GTK_CONTAINER(hb));
      if(g_list_length(lw2) > 2)
      {
        GtkWidget *left = (GtkWidget *)g_list_nth_data(lw2, 0);
        GtkWidget *right = (GtkWidget *)g_list_nth_data(lw2, 2);
        if(pos == 1)
          gtk_widget_hide(left);
        else
          gtk_widget_show(left);
        if(pos == max)
          gtk_widget_hide(right);
        else
          gtk_widget_show(right);
      }
    }
    lw = g_list_next(lw);
    pos++;
  }
}

static void _manage_direct_save(dt_lib_module_t *self)
{
  // get all the values
  gchar *params = _preset_to_string(self, FALSE);
  // update the preset in the database
  dt_lib_presets_add(_(CURRENT_PRESET_NAME), self->plugin_name, self->version(), params, strlen(params), FALSE);
  g_free(params);

  // update the preset name
  dt_conf_set_string("plugins/darkroom/modulegroups_preset", _(CURRENT_PRESET_NAME));
  // and we update the gui
  if(!dt_lib_presets_apply(_(CURRENT_PRESET_NAME), self->plugin_name, self->version()))
    dt_lib_presets_apply((gchar *)C_("modulegroup", FALLBACK_PRESET_NAME), self->plugin_name, self->version());
}

static void _manage_direct_module_toggle(GtkWidget *widget, dt_lib_module_t *self)
{
  gchar *module = (gchar *)g_object_get_data(G_OBJECT(widget), "module_op");
  dt_lib_modulegroups_group_t *gr = (dt_lib_modulegroups_group_t *)g_object_get_data(G_OBJECT(widget), "group");
  if(g_strcmp0(module, "") == 0) return;

  GList *found_item = g_list_find_custom(gr->modules, module, _iop_compare);
  if(!found_item)
  {
    gr->modules = g_list_append(gr->modules, g_strdup(module));
  }
  else
  {
    gr->modules = g_list_delete_link(gr->modules, found_item);
  }

  _manage_direct_save(self);
}

static gint _basics_item_find(gconstpointer a, gconstpointer b)
{
  dt_lib_modulegroups_basic_item_t *ia = (dt_lib_modulegroups_basic_item_t *)a;
  return g_strcmp0(ia->id, (char *)b);
}

static void _manage_direct_basics_module_toggle(GtkWidget *widget, dt_lib_module_t *self)
{
  dt_lib_modulegroups_t *d = (dt_lib_modulegroups_t *)self->data;
  gchar *wid = (gchar *)g_object_get_data(G_OBJECT(widget), "widget_id");
  if(g_strcmp0(wid, "") == 0) return;

  GList *found_item = g_list_find_custom(d->basics, wid, _basics_item_find);

  _basics_hide(self); // to be sure we put back all widget in their right modules

  if(!found_item)
  {
    dt_lib_modulegroups_basic_item_t *item
        = (dt_lib_modulegroups_basic_item_t *)g_malloc0(sizeof(dt_lib_modulegroups_basic_item_t));
    item->id = g_strdup(wid);
    _basics_init_item(item);

    d->basics = g_list_append(d->basics, item);
  }
  else
  {
    GList *l = d->basics;
    while(l)
    {
      dt_lib_modulegroups_basic_item_t *item = (dt_lib_modulegroups_basic_item_t *)l->data;
      if(g_strcmp0(item->id, wid) == 0)
      {
        _basics_free_item(item);
        d->basics = g_list_delete_link(d->basics, l);
        break;
      }
      l = g_list_next(l);
    }
  }

  _manage_direct_save(self);
}

static void _manage_editor_basics_add(GtkWidget *widget, dt_lib_module_t *self)
{
  dt_lib_modulegroups_t *d = (dt_lib_modulegroups_t *)self->data;

  gchar *id = (gchar *)g_object_get_data(G_OBJECT(widget), "widget_id");

  if(!g_list_find_custom(d->edit_basics, id, _basics_item_find))
  {
    dt_lib_modulegroups_basic_item_t *item
        = (dt_lib_modulegroups_basic_item_t *)g_malloc0(sizeof(dt_lib_modulegroups_basic_item_t));
    item->id = g_strdup(id);
    _basics_init_item(item);

    d->edit_basics = g_list_append(d->edit_basics, item);
    _manage_editor_basics_update_list(self);
  }
}

static void _manage_editor_module_add(GtkWidget *widget, dt_lib_module_t *self)
{
  gchar *module = (gchar *)g_object_get_data(G_OBJECT(widget), "module_op");
  dt_lib_modulegroups_group_t *gr = (dt_lib_modulegroups_group_t *)g_object_get_data(G_OBJECT(widget), "group");
  if(g_strcmp0(module, "") == 0) return;

  if(!g_list_find_custom(gr->modules, module, _iop_compare))
  {
    gr->modules = g_list_append(gr->modules, g_strdup(module));
    _manage_editor_module_update_list(self, gr);
  }
}

static int _manage_editor_module_add_sort(gconstpointer a, gconstpointer b)
{
  dt_iop_module_t *ma = (dt_iop_module_t *)a;
  dt_iop_module_t *mb = (dt_iop_module_t *)b;
  gchar *s1 = g_utf8_normalize(ma->name(), -1, G_NORMALIZE_ALL);
  gchar *sa = g_utf8_casefold(s1, -1);
  g_free(s1);
  s1 = g_utf8_normalize(mb->name(), -1, G_NORMALIZE_ALL);
  gchar *sb = g_utf8_casefold(s1, -1);
  g_free(s1);
  const int res = g_strcmp0(sa, sb);
  g_free(sa);
  g_free(sb);
  return res;
}

static int _manage_editor_module_so_add_sort(gconstpointer a, gconstpointer b)
{
  dt_iop_module_so_t *ma = (dt_iop_module_so_t *)a;
  dt_iop_module_so_t *mb = (dt_iop_module_so_t *)b;
  gchar *s1 = g_utf8_normalize(ma->name(), -1, G_NORMALIZE_ALL);
  gchar *sa = g_utf8_casefold(s1, -1);
  g_free(s1);
  s1 = g_utf8_normalize(mb->name(), -1, G_NORMALIZE_ALL);
  gchar *sb = g_utf8_casefold(s1, -1);
  g_free(s1);
  const int res = g_strcmp0(sa, sb);
  g_free(sa);
  g_free(sb);
  return res;
}

static void _manage_module_add_popup(GtkWidget *widget, dt_lib_modulegroups_group_t *gr, GCallback callback,
                                     gpointer data, gboolean toggle)
{
  GtkWidget *pop = gtk_menu_new();
  gtk_widget_set_name(pop, "modulegroups-popup");

  int nbr = 0; // nb of recommended items
  int nba = 0; // nb of already present items
  int nbo = 0; // nb of other items

  GList *m2 = g_list_copy(g_list_first(darktable.iop));
  GList *modules = g_list_sort(m2, _manage_editor_module_so_add_sort);
  while(modules)
  {
    dt_iop_module_so_t *module = (dt_iop_module_so_t *)(modules->data);

    if(!dt_iop_so_is_hidden(module) && !(module->flags() & IOP_FLAGS_DEPRECATED))
    {
      if(!g_list_find_custom(gr->modules, module->op, _iop_compare))
      {
        GtkMenuItem *smi = (GtkMenuItem *)gtk_menu_item_new_with_label(module->name());
        gtk_widget_set_tooltip_text(GTK_WIDGET(smi), _("click to add this module"));
        g_object_set_data(G_OBJECT(smi), "module_op", module->op);
        g_object_set_data(G_OBJECT(smi), "group", gr);
        g_signal_connect(G_OBJECT(smi), "activate", callback, data);

        // does it belong to recommended modules ?
        if(((module->default_group() & IOP_GROUP_BASIC) && g_strcmp0(gr->name, _("base")) == 0)
           || ((module->default_group() & IOP_GROUP_COLOR) && g_strcmp0(gr->name, _("color")) == 0)
           || ((module->default_group() & IOP_GROUP_CORRECT) && g_strcmp0(gr->name, _("correct")) == 0)
           || ((module->default_group() & IOP_GROUP_TONE) && g_strcmp0(gr->name, _("tone")) == 0)
           || ((module->default_group() & IOP_GROUP_EFFECT)
               && g_strcmp0(gr->name, C_("modulegroup", "effect")) == 0)
           || ((module->default_group() & IOP_GROUP_TECHNICAL) && g_strcmp0(gr->name, _("technical")) == 0)
           || ((module->default_group() & IOP_GROUP_GRADING) && g_strcmp0(gr->name, _("grading")) == 0)
           || ((module->default_group() & IOP_GROUP_EFFECTS)
               && g_strcmp0(gr->name, C_("modulegroup", "effects")) == 0))
        {
          gtk_menu_shell_insert(GTK_MENU_SHELL(pop), GTK_WIDGET(smi), nba);
          nbr++;
        }
        else
        {
          gtk_menu_shell_insert(GTK_MENU_SHELL(pop), GTK_WIDGET(smi), nba + nbr);
          nbo++;
        }
      }
      else if(toggle)
      {
        GtkMenuItem *smi = (GtkMenuItem *)gtk_menu_item_new_with_label(module->name());
        gtk_widget_set_tooltip_text(GTK_WIDGET(smi), _("click to remove this module"));
        g_object_set_data(G_OBJECT(smi), "module_op", module->op);
        g_object_set_data(G_OBJECT(smi), "group", gr);
        g_signal_connect(G_OBJECT(smi), "activate", callback, data);
        gtk_menu_shell_insert(GTK_MENU_SHELL(pop), GTK_WIDGET(smi), 0);
        nba++;
      }
    }
    modules = g_list_next(modules);
  }
  g_list_free(m2);

  if((toggle && nba > 0) || nbr > 0)
  {
    GtkWidget *smt = gtk_menu_item_new_with_label(ngettext("other", "others", nbo));
    gtk_widget_set_name(smt, "modulegroups-popup-title");
    gtk_widget_set_sensitive(smt, FALSE);
    gtk_menu_shell_insert(GTK_MENU_SHELL(pop), smt, nba + nbr);
    if(nba + nbr > 0) gtk_menu_shell_insert(GTK_MENU_SHELL(pop), gtk_separator_menu_item_new(), nba + nbr);
  }
  if(nbr > 0)
  {
    GtkWidget *smt = gtk_menu_item_new_with_label(ngettext("recommended module", "recommended modules", nbr));
    gtk_widget_set_name(smt, "modulegroups-popup-title");
    gtk_widget_set_sensitive(smt, FALSE);
    gtk_menu_shell_insert(GTK_MENU_SHELL(pop), smt, nba);
    if(nba > 0) gtk_menu_shell_insert(GTK_MENU_SHELL(pop), gtk_separator_menu_item_new(), nba);
  }
  if(toggle && nba > 0)
  {
    GtkWidget *smt
        = gtk_menu_item_new_with_label(ngettext("already present module", "already present modules", nba));
    gtk_widget_set_name(smt, "modulegroups-popup-title");
    gtk_widget_set_sensitive(smt, FALSE);
    gtk_menu_shell_insert(GTK_MENU_SHELL(pop), smt, 0);
  }

  gtk_widget_show_all(pop);

#if GTK_CHECK_VERSION(3, 22, 0)
  gtk_menu_popup_at_pointer(GTK_MENU(pop), NULL);
#else
  gtk_menu_popup(GTK_MENU(pop), NULL, NULL, NULL, NULL, 0, 0);
#endif
}

static void _manage_basics_add_popup(GtkWidget *widget, GCallback callback, dt_lib_module_t *self, gboolean toggle)
{
  dt_lib_modulegroups_t *d = (dt_lib_modulegroups_t *)self->data;
  GtkWidget *pop = gtk_menu_new();
  gtk_widget_set_name(pop, "modulegroups-popup");

  int nbr = 0; // nb of recommended items
  int nba = 0; // nb of already present items
  int nbo = 0; // nb of other items
  GList *m2 = g_list_copy(g_list_first(darktable.develop->iop));
  GList *modules = g_list_sort(m2, _manage_editor_module_add_sort);
  while(modules)
  {
    dt_iop_module_t *module = (dt_iop_module_t *)modules->data;

    if(!dt_iop_is_hidden(module) && !(module->flags() & IOP_FLAGS_DEPRECATED)
       && (module->multi_priority <= 0
           || g_list_find_custom(darktable.develop->iop, module, _manage_editor_module_find_multi) == NULL))
    {
      // be sure the module is already in one of the "classic" groups
      // we don't want a widget without its "real" module aside
      gboolean exists = FALSE;
      GList *l = toggle ? d->groups : d->edit_groups;
      while(l)
      {
        dt_lib_modulegroups_group_t *gr = (dt_lib_modulegroups_group_t *)l->data;
        if(g_list_find_custom(gr->modules, module->op, _iop_compare))
        {
          exists = TRUE;
          break;
        }
        l = g_list_next(l);
      }
      if(exists)
      {
        // create submenu for module
        GtkMenuItem *smi = (GtkMenuItem *)gtk_menu_item_new_with_label(module->name());
        GtkMenu *sm = (GtkMenu *)gtk_menu_new();
        gtk_menu_item_set_submenu(smi, GTK_WIDGET(sm));
        int nb = 0;

        // let's add the on-off button
        if(!module->hide_enable_button)
        {
          gchar *ws = dt_util_dstrcat(NULL, "|%s|", module->op);
          if(g_list_find_custom(toggle ? d->basics : d->edit_basics, module->op, _basics_item_find))
          {
            if(toggle)
            {
              GtkMenuItem *mi;
              gchar *tx = dt_util_dstrcat(NULL, "%s - %s", module->name(), _("on-off"));
              mi = (GtkMenuItem *)gtk_menu_item_new_with_label(tx);
              g_free(tx);
              gtk_widget_set_tooltip_text(GTK_WIDGET(mi), _("click to remove the widget"));
              g_object_set_data(G_OBJECT(mi), "widget_id", module->op);
              g_signal_connect(G_OBJECT(mi), "activate", callback, self);
              gtk_menu_shell_insert(GTK_MENU_SHELL(pop), GTK_WIDGET(mi), nba);
              nba++;
            }
          }
          else
          {
            if(strstr(RECOMMENDED_BASICS, ws))
            {
              GtkMenuItem *mi;
              gchar *tx = dt_util_dstrcat(NULL, "%s - %s", module->name(), _("on-off"));
              mi = (GtkMenuItem *)gtk_menu_item_new_with_label(tx);
              g_free(tx);
              gtk_widget_set_tooltip_text(GTK_WIDGET(mi), _("click to add the widget"));
              g_object_set_data(G_OBJECT(mi), "widget_id", module->op);
              g_signal_connect(G_OBJECT(mi), "activate", callback, self);
              gtk_menu_shell_insert(GTK_MENU_SHELL(pop), GTK_WIDGET(mi), nba + nbr);
              nbr++;
            }
            GtkMenuItem *mii;
            mii = (GtkMenuItem *)gtk_menu_item_new_with_label(_("on-off"));
            gtk_widget_set_tooltip_text(GTK_WIDGET(mii), _("click to add the widget"));
            g_object_set_data(G_OBJECT(mii), "widget_id", module->op);
            g_signal_connect(G_OBJECT(mii), "activate", callback, self);
            gtk_menu_shell_append(GTK_MENU_SHELL(sm), GTK_WIDGET(mii));
            nb++;
            nbo++;
          }
          g_free(ws);
        }

        // let's go throught all widgets from this module
        GList *la = g_list_last(darktable.control->accelerator_list);
        while(la)
        {
          dt_accel_t *accel = (dt_accel_t *)la->data;
          gchar *pre = dt_util_dstrcat(NULL, "<Darktable>/image operations/%s/", module->op);
          if(accel && accel->closure && accel->closure->data && g_str_has_prefix(accel->path, pre)
             && g_str_has_suffix(accel->path, "/dynamic"))
          {
            gchar *wid = NULL;
            gchar *wn = NULL;
            _basics_get_names_from_accel_path(accel->path, &wid, NULL, &wn);
            gchar *ws = dt_util_dstrcat(NULL, "|%s|", wid);
            if(g_list_find_custom(toggle ? d->basics : d->edit_basics, wid, _basics_item_find))
            {
              if(toggle)
              {
                GtkMenuItem *mi;
                gchar *tx = dt_util_dstrcat(NULL, "%s - %s", module->name(), wn);
                mi = (GtkMenuItem *)gtk_menu_item_new_with_label(tx);
                g_free(tx);
                gtk_widget_set_tooltip_text(GTK_WIDGET(mi), _("click to remove the widget"));
                g_object_set_data_full(G_OBJECT(mi), "widget_id", g_strdup(wid), g_free);
                g_signal_connect(G_OBJECT(mi), "activate", callback, self);
                gtk_menu_shell_insert(GTK_MENU_SHELL(pop), GTK_WIDGET(mi), nba);
                nba++;
              }
            }
            else
            {
              if(strstr(RECOMMENDED_BASICS, ws))
              {
                GtkMenuItem *mi;
                gchar *tx = dt_util_dstrcat(NULL, "%s - %s", module->name(), wn);
                mi = (GtkMenuItem *)gtk_menu_item_new_with_label(tx);
                g_free(tx);
                gtk_widget_set_tooltip_text(GTK_WIDGET(mi), _("click to add the widget"));
                g_object_set_data_full(G_OBJECT(mi), "widget_id", g_strdup(wid), g_free);
                g_signal_connect(G_OBJECT(mi), "activate", callback, self);
                gtk_menu_shell_insert(GTK_MENU_SHELL(pop), GTK_WIDGET(mi), nba + nbr);
                nbr++;
              }
              GtkMenuItem *mii;
              mii = (GtkMenuItem *)gtk_menu_item_new_with_label(wn);
              gtk_widget_set_tooltip_text(GTK_WIDGET(mii), _("click to add the widget"));
              g_object_set_data_full(G_OBJECT(mii), "widget_id", g_strdup(wid), g_free);
              g_signal_connect(G_OBJECT(mii), "activate", callback, self);
              gtk_menu_shell_append(GTK_MENU_SHELL(sm), GTK_WIDGET(mii));
              nb++;
              nbo++;
            }
            g_free(wid);
            g_free(wn);
            g_free(ws);
          }
          g_free(pre);
          la = g_list_previous(la);
        }
        // add submenu to main menu if we got any widgets
        if(nb > 0) gtk_menu_shell_append(GTK_MENU_SHELL(pop), GTK_WIDGET(smi));
      }
    }
    modules = g_list_next(modules);
  }
  g_list_free(m2);

  // we add the titles if we have recommended widgets
  if((toggle && nba > 0) || nbr > 0)
  {
    GtkWidget *smt = gtk_menu_item_new_with_label(ngettext("other", "others", nbo));
    gtk_widget_set_name(smt, "modulegroups-popup-title");
    gtk_widget_set_sensitive(smt, FALSE);
    gtk_menu_shell_insert(GTK_MENU_SHELL(pop), smt, nba + nbr);
    if(nba + nbr > 0) gtk_menu_shell_insert(GTK_MENU_SHELL(pop), gtk_separator_menu_item_new(), nba + nbr);
  }
  if(nbr > 0)
  {
    GtkWidget *smt = gtk_menu_item_new_with_label(ngettext("recommended widget", "recommended widgets", nbr));
    gtk_widget_set_name(smt, "modulegroups-popup-title");
    gtk_widget_set_sensitive(smt, FALSE);
    gtk_menu_shell_insert(GTK_MENU_SHELL(pop), smt, nba);
    if(nba > 0) gtk_menu_shell_insert(GTK_MENU_SHELL(pop), gtk_separator_menu_item_new(), nba);
  }
  if(toggle && nba > 0)
  {
    GtkWidget *smt
        = gtk_menu_item_new_with_label(ngettext("already present widget", "already present widgets", nba));
    gtk_widget_set_name(smt, "modulegroups-popup-title");
    gtk_widget_set_sensitive(smt, FALSE);
    gtk_menu_shell_insert(GTK_MENU_SHELL(pop), smt, 0);
  }

  gtk_widget_show_all(pop);

#if GTK_CHECK_VERSION(3, 22, 0)
  gtk_menu_popup_at_pointer(GTK_MENU(pop), NULL);
#else
  gtk_menu_popup(GTK_MENU(pop), NULL, NULL, NULL, NULL, 0, 0);
#endif
}

static void _manage_editor_basics_add_popup(GtkWidget *widget, GdkEvent *event, dt_lib_module_t *self)
{
  _manage_basics_add_popup(widget, G_CALLBACK(_manage_editor_basics_add), self, FALSE);
}

static void _manage_editor_module_add_popup(GtkWidget *widget, GdkEventButton *event, dt_lib_module_t *self)
{
  dt_lib_modulegroups_group_t *gr = (dt_lib_modulegroups_group_t *)g_object_get_data(G_OBJECT(widget), "group");
  _manage_module_add_popup(widget, gr, G_CALLBACK(_manage_editor_module_add), self, FALSE);
}

static gboolean _manage_direct_popup(GtkWidget *widget, GdkEventButton *event, dt_lib_module_t *self)
{
  if(event->type == GDK_BUTTON_PRESS && event->button == 3)
  {
    dt_lib_modulegroups_group_t *gr = (dt_lib_modulegroups_group_t *)g_object_get_data(G_OBJECT(widget), "group");
    if(!g_strcmp0(gr->name, C_("modulegroup", "deprecated"))) return FALSE;
    _manage_module_add_popup(widget, gr, G_CALLBACK(_manage_direct_module_toggle), self, TRUE);
    return TRUE;
  }
  return FALSE;
}

static gboolean _manage_direct_basic_popup(GtkWidget *widget, GdkEventButton *event, dt_lib_module_t *self)
{
  if(event->type == GDK_BUTTON_PRESS && event->button == 3)
  {
    _manage_basics_add_popup(widget, G_CALLBACK(_manage_direct_basics_module_toggle), self, TRUE);
    return TRUE;
  }
  return FALSE;
}

void gui_init(dt_lib_module_t *self)
{
  /* initialize ui widgets */
  dt_lib_modulegroups_t *d = (dt_lib_modulegroups_t *)g_malloc0(sizeof(dt_lib_modulegroups_t));
  self->data = (void *)d;

  self->widget = gtk_box_new(GTK_ORIENTATION_VERTICAL, 0);
  dt_gui_add_help_link(self->widget, dt_get_help_url(self->plugin_name));
  gtk_widget_set_name(self->widget, "modules-tabs");

  dtgtk_cairo_paint_flags_t pf = CPF_STYLE_FLAT;

  d->hbox_buttons = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 0);
  d->hbox_search_box = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 0);

  // groups
  d->hbox_groups = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 0);
  gtk_box_pack_start(GTK_BOX(d->hbox_buttons), d->hbox_groups, TRUE, TRUE, 0);

  // basic group button
  d->basic_btn = dtgtk_togglebutton_new(dtgtk_cairo_paint_modulegroup_basics, pf, NULL);
  g_signal_connect(d->basic_btn, "button-press-event", G_CALLBACK(_manage_direct_basic_popup), self);
  g_signal_connect(d->basic_btn, "toggled", G_CALLBACK(_lib_modulegroups_toggle), self);
  gtk_widget_set_tooltip_text(d->basic_btn, _("show basic adjustement list"));
  gtk_box_pack_start(GTK_BOX(d->hbox_groups), d->basic_btn, TRUE, TRUE, 0);

  d->vbox_basic = NULL;
  d->basics = NULL;

  // active group button
  d->active_btn = dtgtk_togglebutton_new(dtgtk_cairo_paint_modulegroup_active, pf, NULL);
  g_signal_connect(d->active_btn, "toggled", G_CALLBACK(_lib_modulegroups_toggle), self);
  gtk_widget_set_tooltip_text(d->active_btn, _("show only active modules"));
  gtk_box_pack_start(GTK_BOX(d->hbox_groups), d->active_btn, TRUE, TRUE, 0);

  // we load now the presets btn
  self->presets_button = dtgtk_button_new(dtgtk_cairo_paint_presets, CPF_STYLE_FLAT, NULL);
  gtk_widget_set_tooltip_text(self->presets_button, _("presets"));
  gtk_box_pack_start(GTK_BOX(d->hbox_buttons), self->presets_button, FALSE, FALSE, 0);

  /* search box */
  GtkWidget *label = gtk_label_new(_("search module"));
  gtk_box_pack_start(GTK_BOX(d->hbox_search_box), label, FALSE, TRUE, 0);

  d->text_entry = gtk_entry_new();
  gtk_widget_add_events(d->text_entry, GDK_FOCUS_CHANGE_MASK);

  gtk_widget_set_tooltip_text(d->text_entry, _("search modules by name or tag"));
  gtk_widget_add_events(d->text_entry, GDK_KEY_PRESS_MASK);
  g_signal_connect(G_OBJECT(d->text_entry), "changed", G_CALLBACK(_text_entry_changed_callback), self);
  g_signal_connect(G_OBJECT(d->text_entry), "icon-press", G_CALLBACK(_text_entry_icon_press_callback), self);
  g_signal_connect(G_OBJECT(d->text_entry), "key-press-event", G_CALLBACK(_text_entry_key_press_callback), self);
  gtk_box_pack_start(GTK_BOX(d->hbox_search_box), d->text_entry, TRUE, TRUE, 0);
  gtk_entry_set_width_chars(GTK_ENTRY(d->text_entry), 0);
  gtk_entry_set_icon_from_icon_name(GTK_ENTRY(d->text_entry), GTK_ENTRY_ICON_SECONDARY, "edit-clear");
  gtk_entry_set_icon_tooltip_text(GTK_ENTRY(d->text_entry), GTK_ENTRY_ICON_SECONDARY, _("clear text"));
  gtk_widget_set_name(GTK_WIDGET(d->hbox_search_box), "search-box");


  gtk_box_pack_start(GTK_BOX(self->widget), d->hbox_buttons, TRUE, TRUE, 0);
  gtk_box_pack_start(GTK_BOX(self->widget), d->hbox_search_box, TRUE, TRUE, 0);

  // deprecated message
  d->deprecated
      = gtk_label_new(_("following modules are deprecated because they have internal design mistakes"
                        " that can't be solved and alternatives that solve them.\nthey will be removed for"
                        " new edits in next release."));
  gtk_widget_set_name(d->deprecated, "modulegroups-deprecated-msg");
  gtk_label_set_line_wrap(GTK_LABEL(d->deprecated), TRUE);
  gtk_box_pack_start(GTK_BOX(self->widget), d->deprecated, TRUE, TRUE, 0);

  gtk_toggle_button_set_active(GTK_TOGGLE_BUTTON(d->active_btn), TRUE);
  d->current = dt_conf_get_int("plugins/darkroom/groups");
  if(d->current == DT_MODULEGROUP_NONE) _lib_modulegroups_update_iop_visibility(self);
  gtk_widget_show_all(self->widget);
  gtk_widget_show_all(d->hbox_buttons);
  gtk_widget_set_no_show_all(d->hbox_buttons, TRUE);
  gtk_widget_show_all(d->hbox_search_box);
  gtk_widget_set_no_show_all(d->hbox_search_box, TRUE);

  /*
   * set the proxy functions
   */
  darktable.develop->proxy.modulegroups.module = self;
  darktable.develop->proxy.modulegroups.set = _lib_modulegroups_set;
  darktable.develop->proxy.modulegroups.update_visibility = _lib_modulegroups_update_visibility_proxy;
  darktable.develop->proxy.modulegroups.get = _lib_modulegroups_get;
  darktable.develop->proxy.modulegroups.test = _lib_modulegroups_test;
  darktable.develop->proxy.modulegroups.switch_group = _lib_modulegroups_switch_group;
  darktable.develop->proxy.modulegroups.search_text_focus = _lib_modulegroups_search_text_focus;
  darktable.develop->proxy.modulegroups.test_visible = _lib_modulegroups_test_visible;
}

static void _buttons_update(dt_lib_module_t *self)
{
  dt_lib_modulegroups_t *d = (dt_lib_modulegroups_t *)self->data;

  // first, we destroy all existing buttons except active one an preset one
  GList *l = gtk_container_get_children(GTK_CONTAINER(d->hbox_groups));
  if(l) l = g_list_next(l); // skip basics group
  if(l) l = g_list_next(l); // skip active group
  while(l)
  {
    GtkWidget *bt = (GtkWidget *)l->data;
    gtk_widget_destroy(bt);
    l = g_list_next(l);
  }
  gtk_widget_set_visible(d->basic_btn, d->basics_show);

  // if there's no groups, we ensure that the preset button is on the search line and we hide the active button
  gtk_widget_set_visible(d->hbox_search_box, d->show_search);
  if(g_list_length(d->groups) == 0 && d->show_search)
  {
    if(gtk_widget_get_parent(self->presets_button) != d->hbox_search_box)
    {
      g_object_ref(self->presets_button);
      gtk_container_remove(GTK_CONTAINER(gtk_widget_get_parent(self->presets_button)), self->presets_button);
      gtk_box_pack_start(GTK_BOX(d->hbox_search_box), self->presets_button, FALSE, FALSE, 0);
      g_object_unref(self->presets_button);
    }
    gtk_widget_hide(d->hbox_buttons);
    d->current = DT_MODULEGROUP_ACTIVE_PIPE;
    _lib_modulegroups_update_iop_visibility(self);
    return;
  }
  else
  {
    if(gtk_widget_get_parent(self->presets_button) != d->hbox_buttons)
    {
      g_object_ref(self->presets_button);
      gtk_container_remove(GTK_CONTAINER(gtk_widget_get_parent(self->presets_button)), self->presets_button);
      gtk_box_pack_start(GTK_BOX(d->hbox_buttons), self->presets_button, FALSE, FALSE, 0);
      g_object_unref(self->presets_button);
    }
    gtk_widget_show(d->hbox_buttons);
    gtk_widget_show(d->hbox_groups);
  }

  // then we repopulate the box with new buttons
  l = d->groups;
  while(l)
  {
    dt_lib_modulegroups_group_t *gr = (dt_lib_modulegroups_group_t *)l->data;
    GtkWidget *bt = dtgtk_togglebutton_new(_buttons_get_icon_fct(gr->icon), CPF_STYLE_FLAT, NULL);
    g_object_set_data(G_OBJECT(bt), "group", gr);
    g_signal_connect(bt, "button-press-event", G_CALLBACK(_manage_direct_popup), self);
    g_signal_connect(bt, "toggled", G_CALLBACK(_lib_modulegroups_toggle), self);
    gtk_widget_set_tooltip_text(bt, gr->name);
    gr->button = bt;
    gtk_box_pack_start(GTK_BOX(d->hbox_groups), bt, TRUE, TRUE, 0);
    gtk_widget_show(bt);
    l = g_list_next(l);
  }

  // last, if d->current still valid, we select it otherwise the first one
  int cur = d->current;
  d->current = DT_MODULEGROUP_NONE;
  if(cur > g_list_length(d->groups) && cur != DT_MODULEGROUP_BASICS) cur = DT_MODULEGROUP_ACTIVE_PIPE;
  if(cur == DT_MODULEGROUP_BASICS && !d->basics_show) cur = DT_MODULEGROUP_ACTIVE_PIPE;
  if(cur == DT_MODULEGROUP_ACTIVE_PIPE)
    gtk_toggle_button_set_active(GTK_TOGGLE_BUTTON(d->active_btn), TRUE);
  else if(cur == DT_MODULEGROUP_BASICS)
  {
    if(gtk_toggle_button_get_active(GTK_TOGGLE_BUTTON(d->basic_btn)))
    {
      // we need to manually refresh the list
      d->current = DT_MODULEGROUP_BASICS;
      _lib_modulegroups_update_iop_visibility(self);
    }
    else
      gtk_toggle_button_set_active(GTK_TOGGLE_BUTTON(d->basic_btn), TRUE);
  }
  else
  {
    dt_lib_modulegroups_group_t *gr = (dt_lib_modulegroups_group_t *)g_list_nth_data(d->groups, cur - 1);
    gtk_toggle_button_set_active(GTK_TOGGLE_BUTTON(gr->button), TRUE);
  }
}

static void _manage_editor_group_move_right(GtkWidget *widget, GdkEventButton *event, dt_lib_module_t *self)
{
  dt_lib_modulegroups_t *d = (dt_lib_modulegroups_t *)self->data;
  dt_lib_modulegroups_group_t *gr = (dt_lib_modulegroups_group_t *)g_object_get_data(G_OBJECT(widget), "group");
  GtkWidget *vb = gtk_widget_get_parent(gtk_widget_get_parent(widget));

  // we move the group inside the list
  const int pos = g_list_index(d->edit_groups, gr);
  if(pos < 0 || pos >= g_list_length(d->edit_groups) - 1) return;
  d->edit_groups = g_list_remove(d->edit_groups, gr);
  d->edit_groups = g_list_insert(d->edit_groups, gr, pos + 1);

  // we move the group in the ui
  gtk_box_reorder_child(GTK_BOX(gtk_widget_get_parent(vb)), vb, pos + 1);
  // and we update arrows
  _manage_editor_group_update_arrows(gtk_widget_get_parent(vb));
}

static void _manage_editor_group_move_left(GtkWidget *widget, GdkEventButton *event, dt_lib_module_t *self)
{
  dt_lib_modulegroups_t *d = (dt_lib_modulegroups_t *)self->data;
  dt_lib_modulegroups_group_t *gr = (dt_lib_modulegroups_group_t *)g_object_get_data(G_OBJECT(widget), "group");
  GtkWidget *vb = gtk_widget_get_parent(gtk_widget_get_parent(widget));

  // we move the group inside the list
  const int pos = g_list_index(d->edit_groups, gr);
  if(pos <= 0) return;
  d->edit_groups = g_list_remove(d->edit_groups, gr);
  d->edit_groups = g_list_insert(d->edit_groups, gr, pos - 1);

  // we move the group in the ui
  gtk_box_reorder_child(GTK_BOX(gtk_widget_get_parent(vb)), vb, pos - 1);
  // and we update arrows
  _manage_editor_group_update_arrows(gtk_widget_get_parent(vb));
}

static void _manage_editor_group_remove(GtkWidget *widget, GdkEventButton *event, dt_lib_module_t *self)
{
  dt_lib_modulegroups_t *d = (dt_lib_modulegroups_t *)self->data;
  dt_lib_modulegroups_group_t *gr = (dt_lib_modulegroups_group_t *)g_object_get_data(G_OBJECT(widget), "group");
  GtkWidget *vb = gtk_widget_get_parent(gtk_widget_get_parent(gtk_widget_get_parent(widget)));
  GtkWidget *groups_box = gtk_widget_get_parent(vb);

  // we remove the group from the list and destroy it
  d->edit_groups = g_list_remove(d->edit_groups, gr);
  g_free(gr->name);
  g_free(gr->icon);
  g_list_free_full(gr->modules, g_free);
  g_free(gr);

  // we remove the group from the ui
  gtk_widget_destroy(vb);

  // and we update arrows
  _manage_editor_group_update_arrows(groups_box);

  // we also cleanup basics widgets list
  _basics_cleanup_list(self, TRUE);
}

static void _manage_editor_group_name_changed(GtkWidget *tb, GdkEventButton *event, dt_lib_module_t *self)
{
  dt_lib_modulegroups_group_t *gr = (dt_lib_modulegroups_group_t *)g_object_get_data(G_OBJECT(tb), "group");
  const gchar *txt = gtk_entry_get_text(GTK_ENTRY(tb));
  g_free(gr->name);
  gr->name = g_strdup(txt);
}

static void _manage_editor_group_icon_changed(GtkWidget *widget, GdkEventButton *event,
                                              dt_lib_modulegroups_group_t *gr)
{
  char *ic = (char *)g_object_get_data(G_OBJECT(widget), "ic_name");
  g_free(gr->icon);
  gr->icon = g_strdup(ic);
  GtkWidget *pop = gtk_widget_get_parent(gtk_widget_get_parent(widget));
  GtkWidget *btn = gtk_popover_get_relative_to(GTK_POPOVER(pop));
  dtgtk_button_set_paint(DTGTK_BUTTON(btn), _buttons_get_icon_fct(ic), CPF_DO_NOT_USE_BORDER | CPF_STYLE_FLAT,
                         NULL);
  gtk_popover_popdown(GTK_POPOVER(pop));
}

static void _manage_editor_group_icon_popup(GtkWidget *btn, GdkEventButton *event, dt_lib_module_t *self)
{
  dt_lib_modulegroups_group_t *gr = (dt_lib_modulegroups_group_t *)g_object_get_data(G_OBJECT(btn), "group");

  GtkWidget *pop = gtk_popover_new(btn);
  GtkWidget *vb = gtk_box_new(GTK_ORIENTATION_VERTICAL, 0);
  gtk_widget_set_name(pop, "modulegroups-icons-popup");

  GtkWidget *eb, *hb, *ic;
  eb = gtk_event_box_new();
  hb = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 0);
  ic = dtgtk_button_new(dtgtk_cairo_paint_modulegroup_basic, CPF_DO_NOT_USE_BORDER | CPF_STYLE_FLAT, NULL);
  gtk_box_pack_start(GTK_BOX(hb), ic, FALSE, TRUE, 0);
  gtk_box_pack_start(GTK_BOX(hb), gtk_label_new(_("basic icon")), TRUE, TRUE, 0);
  g_object_set_data(G_OBJECT(eb), "ic_name", "basic");
  g_signal_connect(G_OBJECT(eb), "button-press-event", G_CALLBACK(_manage_editor_group_icon_changed), gr);
  gtk_container_add(GTK_CONTAINER(eb), hb);
  gtk_box_pack_start(GTK_BOX(vb), eb, FALSE, TRUE, 0);

  eb = gtk_event_box_new();
  hb = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 0);
  ic = dtgtk_button_new(dtgtk_cairo_paint_modulegroup_active, CPF_DO_NOT_USE_BORDER | CPF_STYLE_FLAT, NULL);
  gtk_box_pack_start(GTK_BOX(hb), ic, FALSE, TRUE, 0);
  gtk_box_pack_start(GTK_BOX(hb), gtk_label_new(_("active icon")), TRUE, TRUE, 0);
  g_object_set_data(G_OBJECT(eb), "ic_name", "active");
  g_signal_connect(G_OBJECT(eb), "button-press-event", G_CALLBACK(_manage_editor_group_icon_changed), gr);
  gtk_container_add(GTK_CONTAINER(eb), hb);
  gtk_box_pack_start(GTK_BOX(vb), eb, FALSE, TRUE, 0);

  eb = gtk_event_box_new();
  hb = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 0);
  ic = dtgtk_button_new(dtgtk_cairo_paint_modulegroup_color, CPF_DO_NOT_USE_BORDER | CPF_STYLE_FLAT, NULL);
  gtk_box_pack_start(GTK_BOX(hb), ic, FALSE, TRUE, 0);
  gtk_box_pack_start(GTK_BOX(hb), gtk_label_new(_("color icon")), TRUE, TRUE, 0);
  g_object_set_data(G_OBJECT(eb), "ic_name", "color");
  g_signal_connect(G_OBJECT(eb), "button-press-event", G_CALLBACK(_manage_editor_group_icon_changed), gr);
  gtk_container_add(GTK_CONTAINER(eb), hb);
  gtk_box_pack_start(GTK_BOX(vb), eb, FALSE, TRUE, 0);

  eb = gtk_event_box_new();
  hb = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 0);
  ic = dtgtk_button_new(dtgtk_cairo_paint_modulegroup_correct, CPF_DO_NOT_USE_BORDER | CPF_STYLE_FLAT, NULL);
  gtk_box_pack_start(GTK_BOX(hb), ic, FALSE, TRUE, 0);
  gtk_box_pack_start(GTK_BOX(hb), gtk_label_new(_("correct icon")), TRUE, TRUE, 0);
  g_object_set_data(G_OBJECT(eb), "ic_name", "correct");
  g_signal_connect(G_OBJECT(eb), "button-press-event", G_CALLBACK(_manage_editor_group_icon_changed), gr);
  gtk_container_add(GTK_CONTAINER(eb), hb);
  gtk_box_pack_start(GTK_BOX(vb), eb, FALSE, TRUE, 0);

  eb = gtk_event_box_new();
  hb = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 0);
  ic = dtgtk_button_new(dtgtk_cairo_paint_modulegroup_effect, CPF_DO_NOT_USE_BORDER | CPF_STYLE_FLAT, NULL);
  gtk_box_pack_start(GTK_BOX(hb), ic, FALSE, TRUE, 0);
  gtk_box_pack_start(GTK_BOX(hb), gtk_label_new(_("effect icon")), TRUE, TRUE, 0);
  g_object_set_data(G_OBJECT(eb), "ic_name", "effect");
  g_signal_connect(G_OBJECT(eb), "button-press-event", G_CALLBACK(_manage_editor_group_icon_changed), gr);
  gtk_container_add(GTK_CONTAINER(eb), hb);
  gtk_box_pack_start(GTK_BOX(vb), eb, FALSE, TRUE, 0);

  eb = gtk_event_box_new();
  hb = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 0);
  ic = dtgtk_button_new(dtgtk_cairo_paint_modulegroup_favorites, CPF_DO_NOT_USE_BORDER | CPF_STYLE_FLAT, NULL);
  gtk_box_pack_start(GTK_BOX(hb), ic, FALSE, TRUE, 0);
  gtk_box_pack_start(GTK_BOX(hb), gtk_label_new(_("favorites icon")), TRUE, TRUE, 0);
  g_object_set_data(G_OBJECT(eb), "ic_name", "favorites");
  g_signal_connect(G_OBJECT(eb), "button-press-event", G_CALLBACK(_manage_editor_group_icon_changed), gr);
  gtk_container_add(GTK_CONTAINER(eb), hb);
  gtk_box_pack_start(GTK_BOX(vb), eb, FALSE, TRUE, 0);

  eb = gtk_event_box_new();
  hb = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 0);
  ic = dtgtk_button_new(dtgtk_cairo_paint_modulegroup_tone, CPF_DO_NOT_USE_BORDER | CPF_STYLE_FLAT, NULL);
  gtk_box_pack_start(GTK_BOX(hb), ic, FALSE, TRUE, 0);
  gtk_box_pack_start(GTK_BOX(hb), gtk_label_new(_("tone icon")), TRUE, TRUE, 0);
  g_object_set_data(G_OBJECT(eb), "ic_name", "tone");
  g_signal_connect(G_OBJECT(eb), "button-press-event", G_CALLBACK(_manage_editor_group_icon_changed), gr);
  gtk_container_add(GTK_CONTAINER(eb), hb);
  gtk_box_pack_start(GTK_BOX(vb), eb, FALSE, TRUE, 0);

  eb = gtk_event_box_new();
  hb = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 0);
  ic = dtgtk_button_new(dtgtk_cairo_paint_modulegroup_grading, CPF_DO_NOT_USE_BORDER | CPF_STYLE_FLAT, NULL);
  gtk_box_pack_start(GTK_BOX(hb), ic, FALSE, TRUE, 0);
  gtk_box_pack_start(GTK_BOX(hb), gtk_label_new(_("grading icon")), TRUE, TRUE, 0);
  g_object_set_data(G_OBJECT(eb), "ic_name", "grading");
  g_signal_connect(G_OBJECT(eb), "button-press-event", G_CALLBACK(_manage_editor_group_icon_changed), gr);
  gtk_container_add(GTK_CONTAINER(eb), hb);
  gtk_box_pack_start(GTK_BOX(vb), eb, FALSE, TRUE, 0);

  eb = gtk_event_box_new();
  hb = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 0);
  ic = dtgtk_button_new(dtgtk_cairo_paint_modulegroup_technical, CPF_DO_NOT_USE_BORDER | CPF_STYLE_FLAT, NULL);
  gtk_box_pack_start(GTK_BOX(hb), ic, FALSE, TRUE, 0);
  gtk_box_pack_start(GTK_BOX(hb), gtk_label_new(_("technical icon")), TRUE, TRUE, 0);
  g_object_set_data(G_OBJECT(eb), "ic_name", "technical");
  g_signal_connect(G_OBJECT(eb), "button-press-event", G_CALLBACK(_manage_editor_group_icon_changed), gr);
  gtk_container_add(GTK_CONTAINER(eb), hb);
  gtk_box_pack_start(GTK_BOX(vb), eb, FALSE, TRUE, 0);

  gtk_container_add(GTK_CONTAINER(pop), vb);
  gtk_widget_show_all(pop);
}

static GtkWidget *_manage_editor_group_init_basics_box(dt_lib_module_t *self)
{
  dt_lib_modulegroups_t *d = (dt_lib_modulegroups_t *)self->data;

  GtkWidget *vb2 = gtk_box_new(GTK_ORIENTATION_VERTICAL, 0);
  gtk_widget_set_name(vb2, "modulegroups-groupbox");
  // line to edit the group
  GtkWidget *hb2 = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 0);
  gtk_widget_set_name(hb2, "modulegroups-header");

  GtkWidget *btn = NULL;

  GtkWidget *hb3 = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 0);
  gtk_widget_set_name(hb3, "modulegroups-header-center");
  gtk_widget_set_hexpand(hb3, TRUE);

  btn = dtgtk_button_new(dtgtk_cairo_paint_modulegroup_basics, CPF_DO_NOT_USE_BORDER, NULL);
  gtk_widget_set_name(btn, "modulegroups-group-icon");
  gtk_widget_set_sensitive(btn, FALSE);
  gtk_box_pack_start(GTK_BOX(hb3), btn, FALSE, TRUE, 0);

  GtkWidget *tb = gtk_entry_new();
  gtk_widget_set_tooltip_text(tb, _("basics widgets"));
  gtk_widget_set_sensitive(tb, FALSE);
  gtk_entry_set_text(GTK_ENTRY(tb), _("basics widgets"));
  gtk_box_pack_start(GTK_BOX(hb3), tb, TRUE, TRUE, 0);

  gtk_box_pack_start(GTK_BOX(hb2), hb3, FALSE, TRUE, 0);

  gtk_box_pack_start(GTK_BOX(vb2), hb2, FALSE, TRUE, 0);

  // choosen widgets
  GtkWidget *vb3 = gtk_box_new(GTK_ORIENTATION_VERTICAL, 0);
  GtkWidget *sw = gtk_scrolled_window_new(NULL, NULL);
  d->edit_basics_box = gtk_box_new(GTK_ORIENTATION_VERTICAL, 0);
  gtk_scrolled_window_set_policy(GTK_SCROLLED_WINDOW(sw), GTK_POLICY_NEVER, GTK_POLICY_AUTOMATIC);
  _manage_editor_basics_update_list(self);
  gtk_box_pack_start(GTK_BOX(vb3), d->edit_basics_box, FALSE, TRUE, 0);

  // '+' button to add new widgets
  if(!d->edit_ro)
  {
    GtkWidget *hb4 = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 0);
    GtkWidget *bt = dtgtk_button_new(dtgtk_cairo_paint_plus_simple,
                                     CPF_DO_NOT_USE_BORDER | CPF_DIRECTION_LEFT | CPF_STYLE_FLAT, NULL);
    gtk_widget_set_tooltip_text(bt, _("add widgets to the list"));
    gtk_widget_set_name(bt, "modulegroups-add-module-btn");
    g_signal_connect(G_OBJECT(bt), "button-press-event", G_CALLBACK(_manage_editor_basics_add_popup), self);
    gtk_widget_set_halign(hb4, GTK_ALIGN_CENTER);
    gtk_box_pack_start(GTK_BOX(hb4), bt, FALSE, FALSE, 0);
    gtk_box_pack_start(GTK_BOX(vb3), hb4, FALSE, FALSE, 0);
  }

  gtk_container_add(GTK_CONTAINER(sw), vb3);
  gtk_box_pack_start(GTK_BOX(vb2), sw, TRUE, TRUE, 0);

  return vb2;
}

static GtkWidget *_manage_editor_group_init_modules_box(dt_lib_module_t *self, dt_lib_modulegroups_group_t *gr)
{
  dt_lib_modulegroups_t *d = (dt_lib_modulegroups_t *)self->data;

  GtkWidget *vb2 = gtk_box_new(GTK_ORIENTATION_VERTICAL, 0);
  gtk_widget_set_name(vb2, "modulegroups-groupbox");
  // line to edit the group
  GtkWidget *hb2 = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 0);
  gtk_widget_set_name(hb2, "modulegroups-header");

  // left arrow (not if pos == 0 which means this is the first group)
  GtkWidget *btn = NULL;
  if(!d->edit_ro)
  {
    btn = dtgtk_button_new(dtgtk_cairo_paint_arrow, CPF_DO_NOT_USE_BORDER | CPF_DIRECTION_RIGHT | CPF_STYLE_FLAT,
                           NULL);
    gtk_widget_set_tooltip_text(btn, _("move group to the left"));
    g_object_set_data(G_OBJECT(btn), "group", gr);
    g_signal_connect(G_OBJECT(btn), "button-press-event", G_CALLBACK(_manage_editor_group_move_left), self);
    gtk_box_pack_start(GTK_BOX(hb2), btn, FALSE, TRUE, 0);
  }

  GtkWidget *hb3 = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 0);
  gtk_widget_set_name(hb3, "modulegroups-header-center");
  gtk_widget_set_hexpand(hb3, TRUE);

  btn = dtgtk_button_new(_buttons_get_icon_fct(gr->icon), CPF_DO_NOT_USE_BORDER, NULL);
  gtk_widget_set_name(btn, "modulegroups-group-icon");
  gtk_widget_set_tooltip_text(btn, _("group icon"));
  gtk_widget_set_sensitive(btn, !d->edit_ro);
  g_signal_connect(G_OBJECT(btn), "button-press-event", G_CALLBACK(_manage_editor_group_icon_popup), self);
  g_object_set_data(G_OBJECT(btn), "group", gr);
  gtk_box_pack_start(GTK_BOX(hb3), btn, FALSE, TRUE, 0);

  GtkWidget *tb = gtk_entry_new();
  gtk_widget_set_tooltip_text(tb, _("group name"));
  g_object_set_data(G_OBJECT(tb), "group", gr);
  gtk_widget_set_sensitive(tb, !d->edit_ro);
  g_signal_connect(G_OBJECT(tb), "changed", G_CALLBACK(_manage_editor_group_name_changed), self);
  gtk_entry_set_text(GTK_ENTRY(tb), gr->name);
  gtk_box_pack_start(GTK_BOX(hb3), tb, TRUE, TRUE, 0);

  if(!d->edit_ro)
  {
    btn = dtgtk_button_new(dtgtk_cairo_paint_cancel, CPF_DO_NOT_USE_BORDER | CPF_STYLE_FLAT, NULL);
    gtk_widget_set_tooltip_text(btn, _("remove group"));
    g_object_set_data(G_OBJECT(btn), "group", gr);
    g_signal_connect(G_OBJECT(btn), "button-press-event", G_CALLBACK(_manage_editor_group_remove), self);
    gtk_box_pack_end(GTK_BOX(hb3), btn, FALSE, TRUE, 0);
  }

  gtk_box_pack_start(GTK_BOX(hb2), hb3, FALSE, TRUE, 0);

  // right arrow (not if pos == -1 which means this is the last group)
  if(!d->edit_ro)
  {
    btn = dtgtk_button_new(dtgtk_cairo_paint_arrow, CPF_DO_NOT_USE_BORDER | CPF_DIRECTION_LEFT | CPF_STYLE_FLAT,
                           NULL);
    gtk_widget_set_tooltip_text(btn, _("move group to the right"));
    g_object_set_data(G_OBJECT(btn), "group", gr);
    g_signal_connect(G_OBJECT(btn), "button-press-event", G_CALLBACK(_manage_editor_group_move_right), self);
    gtk_box_pack_end(GTK_BOX(hb2), btn, FALSE, TRUE, 0);
  }

  gtk_box_pack_start(GTK_BOX(vb2), hb2, FALSE, TRUE, 0);

  // choosen modules
  GtkWidget *vb3 = gtk_box_new(GTK_ORIENTATION_VERTICAL, 0);
  GtkWidget *sw = gtk_scrolled_window_new(NULL, NULL);
  gr->iop_box = gtk_box_new(GTK_ORIENTATION_VERTICAL, 0);
  gtk_scrolled_window_set_policy(GTK_SCROLLED_WINDOW(sw), GTK_POLICY_NEVER, GTK_POLICY_AUTOMATIC);
  _manage_editor_module_update_list(self, gr);
  gtk_box_pack_start(GTK_BOX(vb3), gr->iop_box, FALSE, TRUE, 0);

  // '+' button to add new module
  if(!d->edit_ro)
  {
    GtkWidget *hb4 = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 0);
    GtkWidget *bt = dtgtk_button_new(dtgtk_cairo_paint_plus_simple,
                                     CPF_DO_NOT_USE_BORDER | CPF_DIRECTION_LEFT | CPF_STYLE_FLAT, NULL);
    gtk_widget_set_tooltip_text(bt, _("add module to the list"));
    gtk_widget_set_name(bt, "modulegroups-add-module-btn");
    g_object_set_data(G_OBJECT(bt), "group", gr);
    g_signal_connect(G_OBJECT(bt), "button-press-event", G_CALLBACK(_manage_editor_module_add_popup), self);
    gtk_widget_set_halign(hb4, GTK_ALIGN_CENTER);
    gtk_box_pack_start(GTK_BOX(hb4), bt, FALSE, FALSE, 0);
    gtk_box_pack_start(GTK_BOX(vb3), hb4, FALSE, FALSE, 0);
  }

  gtk_container_add(GTK_CONTAINER(sw), vb3);
  gtk_box_pack_start(GTK_BOX(vb2), sw, TRUE, TRUE, 0);

  return vb2;
}

static void _manage_editor_reset(GtkWidget *widget, GdkEventButton *event, dt_lib_module_t *self)
{
  dt_lib_modulegroups_t *d = (dt_lib_modulegroups_t *)self->data;

  gchar *txt = g_strdup(d->edit_preset);
  _manage_editor_load(txt, self);
  g_free(txt);
}

static void _manage_editor_group_add(GtkWidget *widget, GdkEventButton *event, dt_lib_module_t *self)
{
  dt_lib_modulegroups_t *d = (dt_lib_modulegroups_t *)self->data;
  dt_lib_modulegroups_group_t *gr = (dt_lib_modulegroups_group_t *)g_malloc0(sizeof(dt_lib_modulegroups_group_t));
  gr->name = g_strdup(_("new"));
  gr->icon = g_strdup("basic");
  d->edit_groups = g_list_append(d->edit_groups, gr);

  // we update the group list
  GtkWidget *vb2 = _manage_editor_group_init_modules_box(self, gr);
  gtk_box_pack_start(GTK_BOX(d->preset_groups_box), vb2, FALSE, TRUE, 0);
  gtk_widget_show_all(vb2);

  // and we update arrows
  _manage_editor_group_update_arrows(d->preset_groups_box);
}

static void _manage_editor_basics_toggle(GtkWidget *button, dt_lib_module_t *self)
{
  dt_lib_modulegroups_t *d = (dt_lib_modulegroups_t *)self->data;
  d->edit_basics_show = gtk_toggle_button_get_active(GTK_TOGGLE_BUTTON(button));
  gtk_widget_set_visible(d->edit_basics_groupbox, d->edit_basics_show);
}

static void _manage_editor_load(const char *preset, dt_lib_module_t *self)
{
  dt_lib_modulegroups_t *d = (dt_lib_modulegroups_t *)self->data;

  // if we have a currently edited preset, we save it
  if(d->edit_preset && g_strcmp0(preset, d->edit_preset) != 0)
  {
    _manage_editor_save(self);
  }

  // we remove all widgets from the box
  GList *lw = gtk_container_get_children(GTK_CONTAINER(d->preset_box));
  while(lw)
  {
    GtkWidget *w = (GtkWidget *)lw->data;
    gtk_widget_destroy(w);
    lw = g_list_next(lw);
  }

  // we update all the preset lines
  lw = gtk_container_get_children(GTK_CONTAINER(d->presets_list));
  while(lw)
  {
    GtkWidget *w = (GtkWidget *)lw->data;
    char *pr_name = g_strdup((char *)g_object_get_data(G_OBJECT(w), "preset_name"));
    if(g_strcmp0(pr_name, preset) == 0)
      gtk_widget_set_name(w, "modulegroups-preset-activated");
    else if(pr_name)
      gtk_widget_set_name(w, "modulegroups-preset");
    lw = g_list_next(lw);
  }

  // get all presets groups
  if(d->edit_groups) _manage_editor_groups_cleanup(self, TRUE);
  if(d->edit_preset) g_free(d->edit_preset);
  d->edit_groups = NULL;
  d->edit_preset = NULL;
  sqlite3_stmt *stmt;
  DT_DEBUG_SQLITE3_PREPARE_V2(
      dt_database_get(darktable.db),
      "SELECT writeprotect, op_params"
      " FROM data.presets"
      " WHERE operation = ?1 AND op_version = ?2 AND name = ?3",
      -1, &stmt, NULL);
  DT_DEBUG_SQLITE3_BIND_TEXT(stmt, 1, self->plugin_name, -1, SQLITE_TRANSIENT);
  DT_DEBUG_SQLITE3_BIND_INT(stmt, 2, self->version());
  DT_DEBUG_SQLITE3_BIND_TEXT(stmt, 3, preset, -1, SQLITE_TRANSIENT);

  if(sqlite3_step(stmt) == SQLITE_ROW)
  {
    d->edit_ro = sqlite3_column_int(stmt, 0);
    const void *blob = sqlite3_column_blob(stmt, 1);
    _preset_from_string(self, (char *)blob, TRUE);
    d->preset_groups_box = NULL; // ensure we don't have any destroyed widget remaining
    d->edit_basics_box = NULL;
    _basics_cleanup_list(self, TRUE);
    d->edit_preset = g_strdup(preset);
    sqlite3_finalize(stmt);
  }
  else
  {
    sqlite3_finalize(stmt);
    return;
  }

  GtkWidget *vb = gtk_box_new(GTK_ORIENTATION_VERTICAL, 0);
  gtk_widget_set_vexpand(vb, TRUE);

  // preset name
  GtkWidget *hb1 = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 0);
  gtk_widget_set_name(hb1, "modulegroups-preset-name");
  gtk_box_pack_start(GTK_BOX(hb1), gtk_label_new(_("preset name : ")), FALSE, TRUE, 0);
  d->preset_name = gtk_entry_new();
  gtk_widget_set_tooltip_text(d->preset_name, _("preset name"));
  gtk_entry_set_text(GTK_ENTRY(d->preset_name), preset);
  gtk_widget_set_sensitive(d->preset_name, !d->edit_ro);
  gtk_box_pack_start(GTK_BOX(hb1), d->preset_name, FALSE, TRUE, 0);
  gtk_box_pack_start(GTK_BOX(vb), hb1, FALSE, TRUE, 0);

  // show search checkbox
  d->edit_search_cb = gtk_check_button_new_with_label(_("show search line"));
  gtk_toggle_button_set_active(GTK_TOGGLE_BUTTON(d->edit_search_cb), d->edit_show_search);
  gtk_widget_set_sensitive(d->edit_search_cb, !d->edit_ro);
  gtk_box_pack_start(GTK_BOX(vb), d->edit_search_cb, FALSE, TRUE, 0);

  // show basics checkbox
  d->basics_chkbox = gtk_check_button_new_with_label(_("show basics widgets group"));
  gtk_toggle_button_set_active(GTK_TOGGLE_BUTTON(d->basics_chkbox), d->edit_basics_show);
  g_signal_connect(G_OBJECT(d->basics_chkbox), "toggled", G_CALLBACK(_manage_editor_basics_toggle), self);
  gtk_widget_set_sensitive(d->basics_chkbox, !d->edit_ro);
  gtk_box_pack_start(GTK_BOX(vb), d->basics_chkbox, FALSE, TRUE, 0);

  hb1 = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 0);
  d->preset_groups_box = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 0);
  gtk_widget_set_name(hb1, "modulegroups-groups-title");
  gtk_box_pack_start(GTK_BOX(hb1), gtk_label_new(_("module groups")), FALSE, TRUE, 0);
  if(!d->edit_ro)
  {
    GtkWidget *bt = dtgtk_button_new(dtgtk_cairo_paint_plus_simple,
                                     CPF_DO_NOT_USE_BORDER | CPF_DIRECTION_LEFT | CPF_STYLE_FLAT, NULL);
    g_signal_connect(G_OBJECT(bt), "button-press-event", G_CALLBACK(_manage_editor_group_add), self);
    gtk_box_pack_start(GTK_BOX(hb1), bt, FALSE, FALSE, 0);
  }
  gtk_widget_set_halign(hb1, GTK_ALIGN_CENTER);
  gtk_box_pack_start(GTK_BOX(vb), hb1, FALSE, TRUE, 0);

  gtk_widget_set_name(d->preset_groups_box, "modulegroups-groups-box");
  // set up basics widgets
  d->edit_basics_groupbox = _manage_editor_group_init_basics_box(self);
  gtk_box_pack_start(GTK_BOX(d->preset_groups_box), d->edit_basics_groupbox, FALSE, TRUE, 0);
  gtk_widget_show_all(d->edit_basics_groupbox);
  gtk_widget_set_no_show_all(d->edit_basics_groupbox, TRUE);
  gtk_widget_set_visible(d->edit_basics_groupbox, d->edit_basics_show);

  // other groups
  GList *l = d->edit_groups;
  while(l)
  {
    dt_lib_modulegroups_group_t *gr = (dt_lib_modulegroups_group_t *)l->data;
    GtkWidget *vb2 = _manage_editor_group_init_modules_box(self, gr);
    gtk_box_pack_start(GTK_BOX(d->preset_groups_box), vb2, FALSE, TRUE, 0);
    l = g_list_next(l);
  }

  gtk_widget_set_halign(d->preset_groups_box, GTK_ALIGN_CENTER);
  GtkWidget *sw = gtk_scrolled_window_new(NULL, NULL);
  gtk_scrolled_window_set_policy(GTK_SCROLLED_WINDOW(sw), GTK_POLICY_AUTOMATIC, GTK_POLICY_NEVER);
  gtk_container_add(GTK_CONTAINER(sw), d->preset_groups_box);
  gtk_box_pack_start(GTK_BOX(vb), sw, TRUE, TRUE, 0);

  // read-only message
  if(d->edit_ro)
  {
    GtkWidget *lb
        = gtk_label_new(_("this is a built-in read-only preset. duplicate it if you want to make changes"));
    gtk_widget_set_name(lb, "modulegroups-ro");
    gtk_box_pack_start(GTK_BOX(vb), lb, FALSE, TRUE, 0);
  }

  // reset button
  if(!d->edit_ro)
  {
    hb1 = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 0);
    GtkWidget *bt = gtk_button_new();
    gtk_widget_set_name(bt, "modulegroups-reset");
    gtk_button_set_label(GTK_BUTTON(bt), _("reset"));
    g_signal_connect(G_OBJECT(bt), "button-press-event", G_CALLBACK(_manage_editor_reset), self);
    gtk_box_pack_end(GTK_BOX(hb1), bt, FALSE, TRUE, 0);
    gtk_box_pack_start(GTK_BOX(vb), hb1, FALSE, TRUE, 0);
  }

  gtk_container_add(GTK_CONTAINER(d->preset_box), vb);
  gtk_widget_show_all(d->preset_box);

  // and we update arrows
  if(!d->edit_ro) _manage_editor_group_update_arrows(d->preset_groups_box);
}

static void _manage_preset_change(GtkWidget *widget, GdkEventButton *event, dt_lib_module_t *self)
{
  const char *preset = (char *)g_object_get_data(G_OBJECT(widget), "preset_name");
  _manage_editor_load(preset, self);
}

static void _manage_preset_add(GtkWidget *widget, GdkEventButton *event, dt_lib_module_t *self)
{
  // find the new name
  sqlite3_stmt *stmt;
  int i = 0;
  gboolean ko = TRUE;
  while(ko)
  {
    i++;
    gchar *tx = dt_util_dstrcat(NULL, "new_%d", i);
    DT_DEBUG_SQLITE3_PREPARE_V2(
        dt_database_get(darktable.db),
        "SELECT name"
        " FROM data.presets"
        " WHERE operation = ?1 AND op_version = ?2 AND name = ?3", -1, &stmt, NULL);
    DT_DEBUG_SQLITE3_BIND_TEXT(stmt, 1, self->plugin_name, -1, SQLITE_TRANSIENT);
    DT_DEBUG_SQLITE3_BIND_INT(stmt, 2, self->version());
    DT_DEBUG_SQLITE3_BIND_TEXT(stmt, 3, tx, -1, SQLITE_TRANSIENT);
    if(sqlite3_step(stmt) != SQLITE_ROW) ko = FALSE;
    sqlite3_finalize(stmt);
    g_free(tx);
  }
  gchar *nname = dt_util_dstrcat(NULL, "new_%d", i);

  // and create a new empty preset
  dt_lib_presets_add(nname, self->plugin_name, self->version(), " ", 1, FALSE);

  _manage_preset_update_list(self);

  // and we load the new preset
  _manage_editor_load(nname, self);
  g_free(nname);
}

static void _manage_preset_duplicate(GtkWidget *widget, GdkEventButton *event, dt_lib_module_t *self)
{
  const char *preset = (char *)g_object_get_data(G_OBJECT(widget), "preset_name");
  gchar *nname = dt_lib_presets_duplicate(preset, self->plugin_name, self->version());

  // reload the window
  _manage_preset_update_list(self);
  // select the duplicated preset
  _manage_editor_load(nname, self);

  g_free(nname);
}

static void _manage_preset_delete(GtkWidget *widget, GdkEventButton *event, dt_lib_module_t *self)
{
  char *preset = (char *)g_object_get_data(G_OBJECT(widget), "preset_name");

  gint res = GTK_RESPONSE_YES;
  GtkWidget *w = gtk_widget_get_toplevel(widget);

  if(dt_conf_get_bool("plugins/lighttable/preset/ask_before_delete_preset"))
  {
    GtkWidget *dialog
        = gtk_message_dialog_new(GTK_WINDOW(w), GTK_DIALOG_DESTROY_WITH_PARENT, GTK_MESSAGE_QUESTION,
                                 GTK_BUTTONS_YES_NO, _("do you really want to delete the preset `%s'?"), preset);
#ifdef GDK_WINDOWING_QUARTZ
    dt_osx_disallow_fullscreen(dialog);
#endif
    gtk_window_set_title(GTK_WINDOW(dialog), _("delete preset?"));
    res = gtk_dialog_run(GTK_DIALOG(dialog));
    gtk_widget_destroy(dialog);
  }

  if(res == GTK_RESPONSE_YES)
  {
    dt_lib_presets_remove(preset, self->plugin_name, self->version());

    // reload presets list
    dt_lib_modulegroups_t *d = (dt_lib_modulegroups_t *)self->data;
    _manage_preset_update_list(self);

    // we try to reload previous selected preset if it still exists
    gboolean sel_ok = FALSE;
    GList *l = gtk_container_get_children(GTK_CONTAINER(d->presets_list));
    while(l)
    {
      GtkWidget *ww = (GtkWidget *)l->data;
      const char *tx = (char *)g_object_get_data(G_OBJECT(ww), "preset_name");
      if(g_strcmp0(tx, gtk_entry_get_text(GTK_ENTRY(d->preset_name))) == 0)
      {
        _manage_editor_load(tx, self);
        sel_ok = TRUE;
        break;
      }
      l = g_list_next(l);
    }
    // otherwise we load the first preset
    if(!sel_ok)
    {
      GtkWidget *ww = (GtkWidget *)g_list_nth_data(gtk_container_get_children(GTK_CONTAINER(d->presets_list)), 0);
      if(ww)
      {
        const char *firstn = (char *)g_object_get_data(G_OBJECT(ww), "preset_name");
        _manage_editor_load(firstn, self);
      }
    }

    // if the deleted preset was the one currently in use, load default preset
    if(dt_conf_key_exists("plugins/darkroom/modulegroups_preset"))
    {
      gchar *cur = dt_conf_get_string("plugins/darkroom/modulegroups_preset");
      if(g_strcmp0(cur, preset) == 0)
      {
        dt_conf_set_string("plugins/darkroom/modulegroups_preset", C_("modulegroup", FALLBACK_PRESET_NAME));
        dt_lib_presets_apply((gchar *)C_("modulegroup", FALLBACK_PRESET_NAME),
                             self->plugin_name, self->version());
      }
      g_free(cur);
    }
  }
}

static gboolean _manage_preset_hover_callback(GtkWidget *widget, GdkEventCrossing *event, gpointer user_data)
{
  int flags = gtk_widget_get_state_flags(gtk_widget_get_parent(widget));

  if(event->type == GDK_ENTER_NOTIFY)
    flags |= GTK_STATE_FLAG_PRELIGHT;
  else
    flags &= ~GTK_STATE_FLAG_PRELIGHT;

  gtk_widget_set_state_flags(gtk_widget_get_parent(widget), flags, TRUE);
  return FALSE;
}

static void _manage_preset_update_list(dt_lib_module_t *self)
{
  dt_lib_modulegroups_t *d = (dt_lib_modulegroups_t *)self->data;

  // we first remove all existing entries from the box
  GList *lw = gtk_container_get_children(GTK_CONTAINER(d->presets_list));
  while(lw)
  {
    GtkWidget *w = (GtkWidget *)lw->data;
    gtk_widget_destroy(w);
    lw = g_list_next(lw);
  }

  // and we repopulate it
  sqlite3_stmt *stmt;
  // order: get shipped defaults first
  DT_DEBUG_SQLITE3_PREPARE_V2(dt_database_get(darktable.db),
                              "SELECT name, writeprotect, description"
                              " FROM data.presets"
                              " WHERE operation=?1 AND op_version=?2"
                              " ORDER BY writeprotect DESC, name, rowid",
                              -1, &stmt, NULL);
  DT_DEBUG_SQLITE3_BIND_TEXT(stmt, 1, self->plugin_name, -1, SQLITE_TRANSIENT);
  DT_DEBUG_SQLITE3_BIND_INT(stmt, 2, self->version());

  while(sqlite3_step(stmt) == SQLITE_ROW)
  {
    const int ro = sqlite3_column_int(stmt, 1);
    const char *name = (char *)sqlite3_column_text(stmt, 0);
    GtkWidget *hb = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 0);
    gtk_widget_set_name(hb, "modulegroups-preset");
    g_object_set_data(G_OBJECT(hb), "preset_name", g_strdup(name));

    // preset label
    GtkWidget *evt = gtk_event_box_new();
    g_object_set_data(G_OBJECT(evt), "preset_name", g_strdup(name));
    g_signal_connect(G_OBJECT(evt), "button-press-event", G_CALLBACK(_manage_preset_change), self);
    g_signal_connect(G_OBJECT(evt), "enter-notify-event", G_CALLBACK(_manage_preset_hover_callback), self);
    g_signal_connect(G_OBJECT(evt), "leave-notify-event", G_CALLBACK(_manage_preset_hover_callback), self);
    GtkWidget *lbl = gtk_label_new(name);
    gtk_widget_set_tooltip_text(lbl, name);
    gtk_widget_set_size_request(lbl, 180, -1);
    gtk_label_set_ellipsize(GTK_LABEL(lbl), PANGO_ELLIPSIZE_END);
    gtk_label_set_xalign(GTK_LABEL(lbl), 0.0);
    gtk_container_add(GTK_CONTAINER(evt), lbl);
    gtk_box_pack_start(GTK_BOX(hb), evt, TRUE, TRUE, 0);

    // duplicate button (not for deprecate preset)
    GtkWidget *btn;
    if(g_strcmp0(name, _(DEPRECATED_PRESET_NAME)))
    {
      btn = dtgtk_button_new(dtgtk_cairo_paint_multiinstance, CPF_STYLE_FLAT, NULL);
      gtk_widget_set_tooltip_text(btn, _("duplicate this preset"));
      g_object_set_data(G_OBJECT(btn), "preset_name", g_strdup(name));
      g_signal_connect(G_OBJECT(btn), "button-press-event", G_CALLBACK(_manage_preset_duplicate), self);
      gtk_box_pack_end(GTK_BOX(hb), btn, FALSE, FALSE, 0);
    }

    // remove button (not for read-lony presets)
    if(!ro)
    {
      btn = dtgtk_button_new(dtgtk_cairo_paint_cancel, CPF_STYLE_FLAT, NULL);
      gtk_widget_set_tooltip_text(btn, _("delete this preset"));
      g_object_set_data(G_OBJECT(btn), "preset_name", g_strdup(name));
      g_signal_connect(G_OBJECT(btn), "button-press-event", G_CALLBACK(_manage_preset_delete), self);
      gtk_box_pack_end(GTK_BOX(hb), btn, FALSE, FALSE, 0);
    }

    gtk_box_pack_start(GTK_BOX(d->presets_list), hb, FALSE, TRUE, 0);
  }
  sqlite3_finalize(stmt);

  // and we finally add the "new preset" button
  GtkWidget *hb2 = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 0);
  GtkWidget *bt = dtgtk_button_new(dtgtk_cairo_paint_plus_simple,
                                   CPF_DO_NOT_USE_BORDER | CPF_DIRECTION_LEFT | CPF_STYLE_FLAT, NULL);
  g_signal_connect(G_OBJECT(bt), "button-press-event", G_CALLBACK(_manage_preset_add), self);
  gtk_widget_set_name(bt, "modulegroups-preset-add-btn");
  gtk_widget_set_tooltip_text(bt, _("add new empty preset"));
  gtk_widget_set_halign(hb2, GTK_ALIGN_CENTER);
  gtk_box_pack_start(GTK_BOX(hb2), bt, FALSE, FALSE, 0);
  gtk_box_pack_start(GTK_BOX(d->presets_list), hb2, FALSE, FALSE, 0);

  gtk_widget_show_all(d->presets_list);
}

static void _manage_editor_destroy(GtkWidget *widget, dt_lib_module_t *self)
{
  dt_lib_modulegroups_t *d = (dt_lib_modulegroups_t *)self->data;

  // we save the last edited preset
  _manage_editor_save(self);

  // and we free editing data
  if(d->edit_groups) _manage_editor_groups_cleanup(self, TRUE);
  if(d->edit_preset) g_free(d->edit_preset);
  d->edit_groups = NULL;
  d->edit_preset = NULL;
}

static void _manage_show_window(dt_lib_module_t *self)
{
  dt_lib_modulegroups_t *d = (dt_lib_modulegroups_t *)self->data;

  GtkWindow *win = GTK_WINDOW(dt_ui_main_window(darktable.gui->ui));
  d->dialog = gtk_dialog_new_with_buttons(_("manage module layouts"), win,
                                          GTK_DIALOG_DESTROY_WITH_PARENT | GTK_DIALOG_MODAL, NULL, NULL);

  gtk_window_set_default_size(GTK_WINDOW(d->dialog), DT_PIXEL_APPLY_DPI(1100), DT_PIXEL_APPLY_DPI(700));

#ifdef GDK_WINDOWING_QUARTZ
  dt_osx_disallow_fullscreen(d->dialog);
#endif
  gtk_widget_set_name(d->dialog, "modulegroups_manager");
  gtk_window_set_title(GTK_WINDOW(d->dialog), _("manage module layouts"));

  // remove the small border
  GtkWidget *content = gtk_dialog_get_content_area(GTK_DIALOG(d->dialog));
  gtk_container_set_border_width(GTK_CONTAINER(content), 0);

  GtkWidget *hb = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 0);
  GtkWidget *vb = gtk_box_new(GTK_ORIENTATION_VERTICAL, 0);
  gtk_widget_set_name(vb, "modulegroups-presets-list");
  d->preset_box = gtk_box_new(GTK_ORIENTATION_VERTICAL, 0);
  gtk_widget_set_name(d->preset_box, "modulegroups_editor");

  GtkWidget *sw = gtk_scrolled_window_new(NULL, NULL);
  gtk_scrolled_window_set_policy(GTK_SCROLLED_WINDOW(sw), GTK_POLICY_NEVER, GTK_POLICY_AUTOMATIC);
  d->presets_list = gtk_box_new(GTK_ORIENTATION_VERTICAL, 0);

  // we load the presets list
  _manage_preset_update_list(self);

  gtk_container_add(GTK_CONTAINER(sw), d->presets_list);
  gtk_box_pack_start(GTK_BOX(vb), sw, TRUE, TRUE, 0);

  gtk_box_pack_start(GTK_BOX(hb), vb, FALSE, TRUE, 0);
  gtk_box_pack_start(GTK_BOX(hb), d->preset_box, TRUE, TRUE, 0);
  gtk_widget_show_all(hb);

  // and we select the current one
  gboolean sel_ok = FALSE;
  if(dt_conf_key_exists("plugins/darkroom/modulegroups_preset"))
  {
    gchar *preset = dt_conf_get_string("plugins/darkroom/modulegroups_preset");
    GList *l = gtk_container_get_children(GTK_CONTAINER(d->presets_list));
    while(l)
    {
      GtkWidget *w = (GtkWidget *)l->data;
      const char *tx = (char *)g_object_get_data(G_OBJECT(w), "preset_name");
      if(g_strcmp0(tx, preset) == 0)
      {
        _manage_editor_load(preset, self);
        sel_ok = TRUE;
        break;
      }
      l = g_list_next(l);
    }
    g_free(preset);
  }
  // or the first one if no selection found
  if(!sel_ok)
  {
    GtkWidget *w = (GtkWidget *)g_list_nth_data(gtk_container_get_children(GTK_CONTAINER(d->presets_list)), 0);
    if(w)
    {
      const char *firstn = (char *)g_object_get_data(G_OBJECT(w), "preset_name");
      _manage_editor_load(firstn, self);
    }
  }

  gtk_container_add(GTK_CONTAINER(gtk_dialog_get_content_area(GTK_DIALOG(d->dialog))), hb);

  g_signal_connect(d->dialog, "destroy", G_CALLBACK(_manage_editor_destroy), self);
  gtk_window_set_resizable(GTK_WINDOW(d->dialog), TRUE);

  gtk_window_set_position(GTK_WINDOW(d->dialog), GTK_WIN_POS_CENTER_ON_PARENT);
  gtk_widget_show(d->dialog);
}


void manage_presets(dt_lib_module_t *self)
{
  _manage_show_window(self);
}

void view_leave(dt_lib_module_t *self, dt_view_t *old_view, dt_view_t *new_view)
{
  if(!strcmp(old_view->module_name, "darkroom"))
  {
    dt_lib_modulegroups_t *d = (dt_lib_modulegroups_t *)self->data;
    dt_gui_key_accel_block_on_focus_disconnect(d->text_entry);
    _basics_hide(self);
  }
}

void view_enter(dt_lib_module_t *self, dt_view_t *old_view, dt_view_t *new_view)
{
  if(!strcmp(new_view->module_name, "darkroom"))
  {
    dt_lib_modulegroups_t *d = (dt_lib_modulegroups_t *)self->data;
    dt_gui_key_accel_block_on_focus_connect(d->text_entry);

    // and we initialize the buttons too
    gchar *preset = dt_conf_get_string("plugins/darkroom/modulegroups_preset");
    if(!dt_lib_presets_apply(preset, self->plugin_name, self->version()))
      dt_lib_presets_apply(_(FALLBACK_PRESET_NAME), self->plugin_name, self->version());
    g_free(preset);

    // and set the current group
    d->current = dt_conf_get_int("plugins/darkroom/groups");
  }
}
#undef PADDING
// modelines: These editor modelines have been set for all relevant files by tools/update_modelines.sh
// vim: shiftwidth=2 expandtab tabstop=2 cindent
// kate: tab-indents: off; indent-width 2; replace-tabs on; indent-mode cstyle; remove-trailing-spaces modified;
