import requests
import mimetypes
import paste.fileapp

import ckan.logic as logic
import ckan.lib.base as base
import ckan.model as model
import ckan.lib.uploader as uploader

from ckan.common import _, request, c, response

import logging
log = logging.getLogger(__name__)

NotFound = logic.NotFound
NotAuthorized = logic.NotAuthorized
get_action = logic.get_action
abort = base.abort
redirect = base.redirect


class S3Controller(base.BaseController):

    def resource_download(self, id, resource_id, filename=None):
        """
        Provide a download by either redirecting the user to the url stored or
        downloading the uploaded file from S3.
        """
        context = {'model': model, 'session': model.Session,
                   'user': c.user or c.author, 'auth_user_obj': c.userobj}

        try:
            rsc = get_action('resource_show')(context, {'id': resource_id})
            get_action('package_show')(context, {'id': id})
        except NotFound:
            abort(404, _('Resource not found'))
        except NotAuthorized:
            abort(401, _('Unauthorized to read resource %s') % id)

        if rsc.get('url_type') == 'upload':
            upload = uploader.get_uploader(rsc)
            try:
                file_url = upload.get_url(rsc['id'])
            except logic.NotFound:
                abort(404, _('Resource not found'))

            try:
                r = requests.get(file_url, stream=True)
            except Exception as e:
                raise e
            else:
                if r.status_code == 403:
                    # A forbidden response at this stage is possibly because
                    # the resource doesn't exist at the file url.
                    abort(404, _('Resource data not found'))
                dataapp = paste.fileapp.DataApp(r.content)
            finally:
                r.close()

            try:
                status, headers, app_iter = request.call_application(dataapp)
            except OSError:
                abort(404, _('Resource data not found'))

            response.headers.update(dict(headers))
            response.status = status
            content_type, x = mimetypes.guess_type(rsc.get('url', ''))
            if content_type:
                response.headers['Content-Type'] = content_type
            log.info(response.headers)
            return app_iter

        elif 'url' not in rsc:
            abort(404, _('No download is available'))
        redirect(rsc['url'])
